package bencher

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"time"

	"github.com/pterm/pterm"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var (
	TransactionCategories      = []string{"first_sale", "refund", "promotion"}
	MetadataDatabase           = "bench_metadata"
	InsertWorkerCollectionName = "insert_workers"
	InstanceCollectionName     = "bencher_instances"
	BenchDatabase              = "mongo_bench"
	BenchCollection            = "transactions"
)

func RandomTransactionCategory() string {
	index := rand.Intn(len(TransactionCategories))
	return TransactionCategories[index]
}

type Transaction struct {
	ID        int64     `bson:"_id,omitempty"`
	Amount    int       `bson:"amount,omitempty"`
	Category  string    `bson:"category,omitempty"`
	Meta      string    `bson:"meta,omitempty"`
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
	returnChannel        chan *StatResult
	insertWorkers        []*InsertWorker
	PrimaryMongoClient   *mongo.Client
	SecondaryMongoClient *mongo.Client
	MetadataMongoClient  *mongo.Client
	RandomStrings        []string
}

type StatResult struct {
	numOps     int
	timeMicros int
	opType     string
	errors     []string
}

func NewBencher(ctx context.Context, config *Config) *BencherInstance {
	inputChannel := make(chan *StatResult)
	bencher := &BencherInstance{
		ID:            primitive.NewObjectID(),
		IsPrimary:     false, // Assume false until inserted into metadata DB
		ctx:           ctx,
		config:        config,
		returnChannel: inputChannel,
		insertWorkers: []*InsertWorker{},
		RandomStrings: []string{},
	}
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

func getCustomTLSConfig(caFile string) (*tls.Config, error) {
	tlsConfig := new(tls.Config)
	certs, err := ioutil.ReadFile(caFile)

	if err != nil {
		return tlsConfig, err
	}

	tlsConfig.RootCAs = x509.NewCertPool()
	ok := tlsConfig.RootCAs.AppendCertsFromPEM(certs)

	if !ok {
		return tlsConfig, errors.New("Failed parsing pem file")
	}

	return tlsConfig, nil
}

const (
	// Path to the AWS CA file
	caFilePath = "rds-combined-ca-bundle.pem"

	// Timeout operations after N seconds
	connectTimeout  = 5
	queryTimeout    = 30
	password        = "pUMP01wyuUVJo7uqVWRR6Lr5XS7QsK43"
	username        = "hurleytestdocdb20221020"
	clusterEndpoint = "hurley-test-docdb.cluster-c8thwxaiia3a.us-east-1.docdb.amazonaws.com"

	// Which instances to read from
	readPreference = "secondaryPreferred"

	connectionStringTemplate = "mongodb://%s:%s@%s/mongo_bench?tls=true&replicaSet=rs0&readpreference=%s"
)

func (bencher *BencherInstance) makeClient(uri string) *mongo.Client {
	connectionURI := fmt.Sprintf(connectionStringTemplate, username, password, clusterEndpoint, readPreference)

	tlsConfig, err := getCustomTLSConfig(caFilePath)
	if err != nil {
		log.Fatalf("Failed getting TLS configuration: %v", err)
	}

	client, err := mongo.NewClient(options.Client().ApplyURI(connectionURI).SetTLSConfig(tlsConfig).SetRetryWrites(false))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	// Force majority write concerns to ensure secondary reads work more consistently
	// connectionString := options.Client().ApplyURI(uri)
	// connectionString.SetTLSConfig(tlsConfig)
	// connectionString.SetWriteConcern(writeconcern.New(writeconcern.WMajority()))
	// connectionString.SetSocketTimeout(5000 * time.Millisecond)
	// connectionString.SetConnectTimeout(5000 * time.Millisecond)
	// connectionString.SetServerSelectionTimeout(5000 * time.Millisecond)
	// // connectionString := options.Client().ApplyURI(uri).SetWriteConcern(writeconcern.New(writeconcern.W(1)))
	// client, err := mongo.NewClient(connectionString)
	// if err != nil {
	// 	log.Fatal(err)
	// }
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
	index := rand.Intn(len(bencher.insertWorkers))
	return bencher.insertWorkers[index]
}

type MongoOp func() error

func (bencher *BencherInstance) TrackOperations(opType string, fn MongoOp) {
	ticker := time.NewTicker(time.Duration(*bencher.config.StatTickSpeedMillis) * time.Millisecond)
	numOps := 0
	totalTimeMicros := 0
	errors := []string{}

	for {
		select {
		case <-ticker.C:
			bencher.returnChannel <- &StatResult{
				numOps:     numOps,
				timeMicros: totalTimeMicros,
				opType:     opType,
				errors:     errors,
			}
			numOps = 0
			totalTimeMicros = 0
			errors = []string{}
		default:
			start := time.Now()
			err := fn()
			totalTimeMicros += int(time.Since(start).Microseconds())
			if err != nil {
				errors = append(errors, fmt.Sprint(err.Error()))
			} else {
				numOps++
			}
		}
	}
}

func (bencher *BencherInstance) SetupDB(client *mongo.Client) error {
	if bencher.IsPrimary {
		index := mongo.IndexModel{
			Keys: bson.D{{Key: "createdat", Value: -1}, {Key: "category", Value: 1}},
		}
		_, err := client.Database(BenchDatabase).Collection(BenchCollection).Indexes().CreateOne(bencher.ctx, index)
		if err != nil {
			return err
		}

		// index = mongo.IndexModel{
		// 	Keys: bson.D{{Key: "_id", Value: "hashed"}},
		// }
		// _, err = client.Database(BenchDatabase).Collection(BenchCollection).Indexes().CreateOne(bencher.ctx, index)
		// if err != nil {
		// 	return err
		// }
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

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func (bencher *BencherInstance) RandomString() string {
	return bencher.RandomStrings[rand.Intn(len(bencher.RandomStrings))]
}

func (bencher *BencherInstance) Start() {
	defer bencher.Close()
	var err error

	for i := 0; i < 1000; i++ {
		bencher.RandomStrings = append(bencher.RandomStrings, RandStringRunes(1024))
	}

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

	for i := 0; i < *bencher.config.NumInsertWorkers; i++ {
		insertWorker := StartInsertWorker(bencher)
		bencher.insertWorkers = append(bencher.insertWorkers, insertWorker)
	}
	for i := 0; i < *bencher.config.NumIDReadWorkers; i++ {
		StartIDReadWorker(bencher)
	}
	for i := 0; i < *bencher.config.NumSecondaryIDReadWorkers; i++ {
		StartSecondaryNodeIDReadWorker(bencher)
	}
	for i := 0; i < *bencher.config.NumUpdateWorkers; i++ {
		StartUpdateWorker(bencher)
	}
	for i := 0; i < *bencher.config.NumAggregationWorkers; i++ {
		StartAggregationWorker(bencher)
	}
	go bencher.StatWorker()

	time.Sleep(100 * time.Minute)
	pterm.Printfln("Benchmark has run its course, exiting...")
}
