package bencher

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/pterm/pterm"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	TransactionCategories      = []string{"first_sale", "refund", "promotion"}
	MetadataDatabase           = "bench_metadata"
	InsertWorkerCollectionName = "insert_workers"
	InstanceCollectionName     = "bencher_instances"
	BenchDatabase              = "mongo_bench"
	BenchCollection            = "transactions"
	NumUsers                   = int64(1_000_000)
)

type Config struct {
	PrimaryURI           *string
	ReaderURI            *string
	MetadataURI          *string
	NumWorkers           *int
	WorkerReadWriteRatio *int
	StatTickSpeedMillis  *int
	Reset                *bool
	Sharded              *bool
}

type DatabaseBencher interface {
	Setup() error
	Close()
	OperationPool() []OperationPool
}

type BencherInstance struct {
	ID               primitive.ObjectID `bson:"_id"`
	IsPrimary        bool               `bson:"isPrimary"`
	MetadataDBClient *mongo.Client      `bson:"-"`
	WorkerManager    *WorkerManager     `bson:"-"`
	RandomStrings    []string           `bson:"-"`

	allInsertWorkers []*InsertWorker `bson:"-"`
	ctx              context.Context `bson:"-"`
	config           *Config         `bson:"-"`
	DatabaseBencher  DatabaseBencher `bson:"-"`
}

const NUMBER_RANDOM_STRINGS = 500000

func NewBencher(ctx context.Context, config *Config) *BencherInstance {
	bencher := &BencherInstance{
		ID:               primitive.NewObjectID(),
		IsPrimary:        false, // Assume false until inserted into metadata DB
		ctx:              ctx,
		config:           config,
		allInsertWorkers: []*InsertWorker{},
		RandomStrings:    generateRandomStrings(100000, 1024),
	}
	go func() {
		for {
			// Regenerate random strings every so often
			time.Sleep(30 * time.Second)
			bencher.RandomStrings = generateRandomStrings(NUMBER_RANDOM_STRINGS, 1024)
		}
	}()
	if strings.HasPrefix(*config.PrimaryURI, "mongodb") {
		bencher.DatabaseBencher = &MongoBencher{
			bencherInstance: bencher,
			ctx:             bencher.ctx,
		}

	} else if strings.HasPrefix(*config.PrimaryURI, "postgres") {
		bencher.DatabaseBencher = &PostgresBencher{
			bencherInstance: bencher,
			ctx:             bencher.ctx,
		}
	} else {
		panic(fmt.Sprintf("Unknown DB type for string: %s", *config.PrimaryURI))
	}
	manager := NewWorkerManager(bencher)
	bencher.WorkerManager = manager
	return bencher
}

type MongoOp func() error

func (bencher *BencherInstance) makeMetadataClient() *mongo.Client {
	if bencher.MetadataDBClient == nil {
		bencher.MetadataDBClient = MakeMongoClient(bencher.ctx, *bencher.config.MetadataURI)
	}
	return bencher.MetadataDBClient
}

func (bencher *BencherInstance) InsertWorkerCollection() *mongo.Collection {
	return bencher.MetadataDBClient.Database(MetadataDatabase).Collection(InsertWorkerCollectionName)
}

func (bencher *BencherInstance) BencherInstanceCollection() *mongo.Collection {
	return bencher.MetadataDBClient.Database(MetadataDatabase).Collection(InstanceCollectionName)
}

func (bencher *BencherInstance) RandomInsertWorker() *InsertWorker {
	for {
		if len(bencher.allInsertWorkers) == 0 {
			time.Sleep(10 * time.Millisecond)
		} else {
			index := rand.Intn(len(bencher.allInsertWorkers))
			return bencher.allInsertWorkers[index]
		}
	}
}

func (bencher *BencherInstance) SetupMetadataDB() error {
	bencher.makeMetadataClient()

	filter := bson.M{"isPrimary": true}
	opts := options.Update()
	opts.SetUpsert(true)
	update := bson.M{
		"$setOnInsert": bson.M{
			"_id":       bencher.ID,
			"isPrimary": true,
		},
	}
	result, err := bencher.BencherInstanceCollection().UpdateOne(context.Background(), filter, update, opts)
	if err != nil {
		return err
	}

	if result.UpsertedID == bencher.ID {
		log.Printf("This instance is the primary")
		bencher.IsPrimary = true
	} else {
		log.Printf("Other primary exists, just starting workers")
		_, err := bencher.BencherInstanceCollection().InsertOne(context.Background(), &bencher)
		if err != nil {
			return err
		}
		bencher.IsPrimary = false
	}

	if bencher.IsPrimary {
		index := mongo.IndexModel{
			Keys:    bson.D{{Key: "workerIndex", Value: 1}},
			Options: options.Index().SetUnique(true),
		}
		_, err = bencher.MetadataDBClient.Database(MetadataDatabase).Collection(InsertWorkerCollectionName).Indexes().CreateOne(bencher.ctx, index)
		if err != nil {
			return err
		}
	}
	return nil
}

func (bencher *BencherInstance) Start() {
	defer bencher.Close()

	log.Println("Setting up metadata db")
	err := bencher.SetupMetadataDB()
	if err != nil {
		log.Fatal("Error setting up metadata mongo connection: ", err)
	}
	err = bencher.DatabaseBencher.Setup()
	if err != nil {
		log.Fatal("Error setting up primary database connection: ", err)
	}
	if *bencher.config.Reset {
		bencher.Reset()
	}

	pools := bencher.DatabaseBencher.OperationPool()
	for _, pool := range pools {
		bencher.WorkerManager.AddPool(pool)
	}

	pterm.Printfln("Starting bench")
	bencher.WorkerManager.Run()

	time.Sleep(100 * time.Minute)
	pterm.Printfln("Benchmark has run its course, exiting...")
}

func (bencher *BencherInstance) Close() {
	bencher.MetadataDBClient.Disconnect(bencher.ctx)
	bencher.DatabaseBencher.Close()
}

func (bencher *BencherInstance) Reset() {
	log.Println("Resetting dbs...")

	err := bencher.MetadataDBClient.Database(MetadataDatabase).Drop(bencher.ctx)
	if err != nil {
		log.Fatal(err)
	}
}

func (bencher *BencherInstance) RandomString() string {
	index := rand.Intn(len(bencher.RandomStrings))
	return bencher.RandomStrings[index]
}

func generateRandomStrings(n int, length int) []string {
	result := make([]string, n)

	for i := 0; i < n; i++ {
		bytes := make([]byte, (length+3)/4*3)
		if _, err := rand.Read(bytes); err != nil {
			panic(err)
		}

		str := base64.RawURLEncoding.EncodeToString(bytes)[:length]
		result[i] = str
	}

	return result
}
