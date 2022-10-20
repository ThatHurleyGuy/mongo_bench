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
)

type Transaction struct {
	ID        int64     `bson:"_id,omitempty"`
	Amount    int       `bson:"amount,omitempty"`
	Category  string    `bson:"category,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
}

type Config struct {
	PrimaryURI                *string
	SecondaryURI              *string
	MetadataURI               *string
	NumInsertWorkers          *int
	NumIDReadWorkers          *int
	NumSecondaryIDReadWorkers *int
	NumAggregationWorkers     *int
	NumUpdateWorkers          *int
	StatTickSpeedMillis       *int
	Reset                     *bool
}

type BencherInstance struct {
	ID        primitive.ObjectID `bson:"_id"`
	IsPrimary bool               `bson:"isPrimary"`

	ctx                  context.Context
	config               *Config
	insertWorkers        []*InsertWorker
	WorkerManager        *WorkerManager
	PrimaryMongoClient   *mongo.Client
	SecondaryMongoClient *mongo.Client
	MetadataMongoClient  *mongo.Client
}

func NewBencher(ctx context.Context, config *Config) *BencherInstance {
	bencher := &BencherInstance{
		ID:            primitive.NewObjectID(),
		IsPrimary:     false, // Assume false until inserted into metadata DB
		ctx:           ctx,
		config:        config,
		insertWorkers: []*InsertWorker{},
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

func (bencher *BencherInstance) SecondaryCollection() *mongo.Collection {
	if bencher.SecondaryMongoClient == nil {
		return nil
	}
	return bencher.SecondaryMongoClient.Database(BenchDatabase).Collection(BenchCollection)
}

func (bencher *BencherInstance) makeClient(uri string) *mongo.Client {
	connectionString := options.Client().ApplyURI(uri)
	connectionString.SetWriteConcern(writeconcern.New(writeconcern.WMajority()))
	connectionString.SetSocketTimeout(5000 * time.Millisecond)
	connectionString.SetConnectTimeout(5000 * time.Millisecond)
	connectionString.SetServerSelectionTimeout(5000 * time.Millisecond)
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

func (bencher *BencherInstance) makeSecondaryClient() *mongo.Client {
	if bencher.SecondaryMongoClient == nil && *bencher.config.SecondaryURI != "" {
		bencher.SecondaryMongoClient = bencher.makeClient(*bencher.config.SecondaryURI)
	}
	return bencher.SecondaryMongoClient
}

func (bencher *BencherInstance) makeMetadataClient() *mongo.Client {
	if bencher.MetadataMongoClient == nil {
		bencher.MetadataMongoClient = bencher.makeClient(*bencher.config.MetadataURI)
	}
	return bencher.MetadataMongoClient
}

func (bencher *BencherInstance) InsertWorkerCollection() *mongo.Collection {
	return bencher.MetadataMongoClient.Database(MetadataDatabase).Collection(InsertWorkerCollectionName)
}

func (bencher *BencherInstance) BencherInstanceCollection() *mongo.Collection {
	return bencher.MetadataMongoClient.Database(MetadataDatabase).Collection(InstanceCollectionName)
}

func (bencher *BencherInstance) RandomInsertWorker() *InsertWorker {
	for {
		if len(bencher.insertWorkers) == 0 {
			time.Sleep(10 * time.Millisecond)
		} else {
			index := rand.Intn(len(bencher.insertWorkers))
			return bencher.insertWorkers[index]
		}
	}
}

type MongoOp func() error

func (bencher *BencherInstance) SetupDB(client *mongo.Client) error {
	if bencher.IsPrimary {
		index := mongo.IndexModel{
			Keys: bson.D{{Key: "createdat", Value: -1}, {Key: "category", Value: 1}},
		}
		_, err := client.Database(BenchDatabase).Collection(BenchCollection).Indexes().CreateOne(bencher.ctx, index)
		if err != nil {
			return err
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
		_, err = bencher.MetadataMongoClient.Database(MetadataDatabase).Collection(InsertWorkerCollectionName).Indexes().CreateOne(bencher.ctx, index)
		if err != nil {
			return err
		}
	}
	return nil
}

func (bencher *BencherInstance) Close() {
	bencher.MetadataMongoClient.Disconnect(bencher.ctx)
	bencher.PrimaryMongoClient.Disconnect(bencher.ctx)
	if bencher.SecondaryMongoClient != nil {
		bencher.SecondaryMongoClient.Disconnect(bencher.ctx)
	}
}

func (bencher *BencherInstance) Reset() {
	log.Println("Resetting dbs...")
	bencher.makePrimaryClient()
	bencher.makeSecondaryClient()
	bencher.makeMetadataClient()
	err := bencher.MetadataMongoClient.Database(MetadataDatabase).Drop(bencher.ctx)
	if err != nil {
		log.Fatal(err)
	}

	err = bencher.PrimaryMongoClient.Database(BenchDatabase).Drop(bencher.ctx)
	if err != nil {
		log.Fatal(err)
	}
	if bencher.SecondaryMongoClient != nil {
		err = bencher.SecondaryMongoClient.Database(BenchDatabase).Drop(bencher.ctx)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func (bencher *BencherInstance) Start() {
	defer bencher.Close()
	var err error

	if *bencher.config.Reset {
		bencher.Reset()
	}

	log.Println("Setting up metadata db")
	bencher.makeMetadataClient()
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

	if *bencher.config.SecondaryURI != "" {
		log.Println("Setting up secondary")
		bencher.makeSecondaryClient()
		err = bencher.SetupDB(bencher.SecondaryMongoClient)
		if err != nil {
			log.Fatal("Error reseting secondary: ", err)
		}
	}

	insertPool := &InsertWorkerPool{bencher: bencher}
	idReadPool := &IDReadWorkerPool{bencher: bencher}
	secondaryIDReadPool := &SecondaryNodeIDReadWorkerPool{bencher: bencher}
	updateWorkerPool := &UpdateWorkerPool{bencher: bencher}
	aggregationPool := &AggregationWorkerPool{bencher: bencher}
	bencher.WorkerManager.AddPool("insert", insertPool)
	bencher.WorkerManager.AddPool("id_read", idReadPool)
	bencher.WorkerManager.AddPool("secondary_node_id_read", secondaryIDReadPool)
	bencher.WorkerManager.AddPool("update", updateWorkerPool)
	bencher.WorkerManager.AddPool("aggregation", aggregationPool)
	bencher.WorkerManager.Run()

	time.Sleep(100 * time.Minute)
	pterm.Printfln("Benchmark has run its course, exiting...")
}
