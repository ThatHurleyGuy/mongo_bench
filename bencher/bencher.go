package bencher

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/pterm/pterm"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
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

type Transaction struct {
	ID        int64     `bson:"_id,omitempty"`
	UserID    int64     `bson:"user_id"`
	Amount    int       `bson:"amount,omitempty"`
	Category  string    `bson:"category,omitempty"`
	CreatedAt time.Time `bson:"created_at"`
}

type Config struct {
	PrimaryURI                *string
	NumInsertWorkers          *int
	NumIDReadWorkers          *int
	NumSecondaryIDReadWorkers *int
	NumAggregationWorkers     *int
	NumUpdateWorkers          *int
	StatTickSpeedMillis       *int
	Reset                     *bool
	Sharded                   *bool
}

type BencherInstance struct {
	ID        primitive.ObjectID `bson:"_id"`
	IsPrimary bool               `bson:"isPrimary"`

	ctx                context.Context
	config             *Config
	allInsertWorkers   []*InsertWorker
	WorkerManager      *WorkerManager
	PrimaryMongoClient *mongo.Client
}

func NewBencher(ctx context.Context, config *Config) *BencherInstance {
	bencher := &BencherInstance{
		ID:               primitive.NewObjectID(),
		IsPrimary:        false, // Assume false until inserted into metadata DB
		ctx:              ctx,
		config:           config,
		allInsertWorkers: []*InsertWorker{},
	}
	manager := NewWorkerManager(bencher)
	bencher.WorkerManager = manager
	return bencher
}

func (bencher *BencherInstance) PrimaryCollection() *mongo.Collection {
	return bencher.PrimaryMongoClient.Database(BenchDatabase).Collection(BenchCollection)
}

func (bencher *BencherInstance) PrimaryCollectionSecondaryRead() *mongo.Collection {
	opts := options.Database().SetReadPreference(readpref.Secondary())
	return bencher.PrimaryMongoClient.Database(BenchDatabase, opts).Collection(BenchCollection)
}

func (bencher *BencherInstance) makeClient(uri string) *mongo.Client {
	connectionString := options.Client().ApplyURI(uri)
	connectionString.SetWriteConcern(writeconcern.New(writeconcern.WMajority()))
	client, err := mongo.NewClient(connectionString)
	if err != nil {
		log.Fatal(err)
	}
	err = client.Connect(bencher.ctx)
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func (bencher *BencherInstance) makePrimaryClient() *mongo.Client {
	if bencher.PrimaryMongoClient == nil {
		bencher.PrimaryMongoClient = bencher.makeClient(*bencher.config.PrimaryURI)
	}
	return bencher.PrimaryMongoClient
}

func (bencher *BencherInstance) InsertWorkerCollection() *mongo.Collection {
	return bencher.PrimaryMongoClient.Database(MetadataDatabase).Collection(InsertWorkerCollectionName)
}

func (bencher *BencherInstance) BencherInstanceCollection() *mongo.Collection {
	return bencher.PrimaryMongoClient.Database(MetadataDatabase).Collection(InstanceCollectionName)
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

type MongoOp func() error

func (bencher *BencherInstance) SetupDB(client *mongo.Client) error {
	if bencher.IsPrimary {
		indexes := []mongo.IndexModel{
			{Keys: bson.D{{Key: "user_id", Value: 1}}},
			{Keys: bson.D{{Key: "user_id", Value: "hashed"}}},
			{Keys: bson.D{{Key: "user_id", Value: 1}, {Key: "created_at", Value: -1}, {Key: "category", Value: 1}}},
			{Keys: bson.D{{Key: "created_at", Value: -1}, {Key: "category", Value: 1}}},
			{Keys: bson.D{{Key: "created_at", Value: -1}}},
		}

		_, err := client.Database(BenchDatabase).Collection(BenchCollection).Indexes().CreateMany(bencher.ctx, indexes)
		if err != nil {
			return err
		}
		if *bencher.config.Sharded {
			result := client.Database("admin").RunCommand(bencher.ctx, bson.M{"enableSharding": BenchDatabase})
			if result.Err() != nil {
				pterm.Printfln("Failed to enable sharding")
				return result.Err()
			}

			result = client.Database("admin").RunCommand(bencher.ctx, bson.D{
				{Key: "shardCollection", Value: "mongo_bench.transactions"},
				{Key: "key", Value: bson.M{"user_id": "hashed"}},
			})
			if result.Err() != nil {
				pterm.Printfln("Failed to shard collection")
				return result.Err()
			}
		}
	}
	return nil
}

func (bencher *BencherInstance) SetupMetadataDB() error {
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
		_, err = bencher.PrimaryMongoClient.Database(MetadataDatabase).Collection(InsertWorkerCollectionName).Indexes().CreateOne(bencher.ctx, index)
		if err != nil {
			return err
		}
	}
	return nil
}

func (bencher *BencherInstance) Close() {
	bencher.PrimaryMongoClient.Disconnect(bencher.ctx)
}

func (bencher *BencherInstance) Reset() {
	log.Println("Resetting dbs...")
	bencher.makePrimaryClient()

	err := bencher.PrimaryMongoClient.Database(MetadataDatabase).Drop(bencher.ctx)
	if err != nil {
		log.Fatal(err)
	}

	err = bencher.PrimaryMongoClient.Database(BenchDatabase).Drop(bencher.ctx)
	if err != nil {
		log.Fatal(err)
	}
}

func (bencher *BencherInstance) Start() {
	defer bencher.Close()
	var err error

	if *bencher.config.Reset {
		bencher.Reset()
	}

	log.Println("Setting up metadata db")
	err = bencher.SetupMetadataDB()
	if err != nil {
		log.Fatal("Error setting up metadata mongo connection: ", err)
	}

	log.Println("Setting up primary")
	bencher.makePrimaryClient()
	err = bencher.SetupDB(bencher.PrimaryMongoClient)
	if err != nil {
		log.Fatal("Error setting up primary: ", err)
	}

	transactionsForUserPool := &TransactionsForUserWorkerPool{bencher: bencher}
	insertPool := &InsertWorkerPool{bencher: bencher}
	idReadPool := &IDReadWorkerPool{bencher: bencher}
	secondaryIDReadPool := &SecondaryNodeIDReadWorkerPool{bencher: bencher}
	updateWorkerPool := &UpdateWorkerPool{bencher: bencher}
	aggregationPool := &AggregationWorkerPool{bencher: bencher}
	bencher.WorkerManager.AddPool("transactions_for_user", *bencher.config.NumSecondaryIDReadWorkers, transactionsForUserPool)
	bencher.WorkerManager.AddPool("insert", *bencher.config.NumInsertWorkers, insertPool)
	bencher.WorkerManager.AddPool("up_to_date_id_read_by_id", *bencher.config.NumIDReadWorkers, idReadPool)
	bencher.WorkerManager.AddPool("unimportant_read_by_id", *bencher.config.NumSecondaryIDReadWorkers, secondaryIDReadPool)
	bencher.WorkerManager.AddPool("update", *bencher.config.NumUpdateWorkers, updateWorkerPool)
	bencher.WorkerManager.AddPool("aggregation", *bencher.config.NumAggregationWorkers, aggregationPool)
	bencher.WorkerManager.Run()

	time.Sleep(100 * time.Minute)
	pterm.Printfln("Benchmark has run its course, exiting...")
}
