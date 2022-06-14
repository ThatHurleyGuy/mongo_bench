package bencher

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/pterm/pterm"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/mongo/driver/uuid"
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
	CreatedAt time.Time `json:"createdAt"`
}

type Config struct {
	PrimaryURI            *string
	SecondaryURI          *string
	MetadataURI           *string
	NumInsertWorkers      *int
	NumIDReadWorkers      *int
	NumAggregationWorkers *int
	NumUpdateWorkers      *int
	StatTickSpeedMillis   *int
}

type BencherInstance struct {
	ID        uuid.UUID `bson:"_id"`
	IsPrimary bool      `bson:"isPrimary"`

	ctx                  context.Context
	config               *Config
	returnChannel        chan FuncResult
	insertWorkerMap      map[int]*InsertWorker
	PrimaryMongoClient   *mongo.Client
	SecondaryMongoClient *mongo.Client
	MetadataMongoClient  *mongo.Client
}

type FuncResult struct {
	numOps     int
	timeMicros int
	opType     string
}

func NewBencher(ctx context.Context, config *Config) *BencherInstance {
	inputChannel := make(chan FuncResult)
	uuid, err := uuid.New()
	if err != nil {
		log.Fatal("Error generating uuid: ", err)
	}
	bencher := &BencherInstance{
		ID:              uuid,
		IsPrimary:       false, // Assume false until inserted into metadata DB
		ctx:             ctx,
		config:          config,
		returnChannel:   inputChannel,
		insertWorkerMap: map[int]*InsertWorker{},
	}
	return bencher
}

func (bencher *BencherInstance) PrimaryCollection() *mongo.Collection {
	return bencher.PrimaryMongoClient.Database(BenchDatabase).Collection(BenchCollection)
}

func (bencher *BencherInstance) SecondaryCollection() *mongo.Collection {
	if bencher.SecondaryMongoClient == nil {
		return nil
	}
	return bencher.SecondaryMongoClient.Database(BenchDatabase).Collection(BenchCollection)
}

func (bencher *BencherInstance) makeClient(uri string) *mongo.Client {
	client, err := mongo.NewClient(options.Client().ApplyURI(uri))
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
	// TODO: speed this up?
	values := make([]*InsertWorker, 0, len(bencher.insertWorkerMap))
	for _, v := range bencher.insertWorkerMap {
		values = append(values, v)
	}
	index := rand.Intn(len(values))
	return values[index]
}

func tableRow(stats []int, numWorkers int, statType string) []string {
	avgSpeed := 0
	perSecond := 0
	if stats[0] > 0 {
		avgSpeed = stats[1] / stats[0]
	}
	if stats[1] > 0 {
		perSecond = int(float64(numWorkers*stats[0]) / float64(float64(stats[1])/1_000_000))
	}
	return []string{statType, fmt.Sprint(perSecond), fmt.Sprint(avgSpeed)}
}

func (bencher *BencherInstance) StatWorker() {
	tickTime := 200
	ticker := time.NewTicker(time.Duration(tickTime) * time.Millisecond)
	stats := []FuncResult{}
	area, err := pterm.DefaultArea.Start()
	if err != nil {
		log.Fatal("Error setting up output area: ", err)
	}

	lastStatBlock := time.Now()
	statMap := map[string][]int{}
	statMap["insert"] = []int{0, 0, 0}
	statMap["id_read"] = []int{0, 0, 0}
	statMap["aggregation"] = []int{0, 0, 0}
	statMap["update"] = []int{0, 0, 0}
	for {
		select {
		case result := <-bencher.returnChannel:
			stats = append(stats, result)
		case <-ticker.C:
			if time.Since(lastStatBlock).Seconds() > 10 {
				lastStatBlock = time.Now()
				statMap = map[string][]int{}
				statMap["insert"] = []int{0, 0, 0}
				statMap["id_read"] = []int{0, 0, 0}
				statMap["aggregation"] = []int{0, 0, 0}
				statMap["update"] = []int{0, 0, 0}
				area.Stop()
				fmt.Println()
				area, err = pterm.DefaultArea.Start()
				if err != nil {
					log.Fatal("Error setting up output area: ", err)
				}
			}

			if len(stats) > 0 {
				for _, v := range stats {
					_, ok := statMap[v.opType]
					if ok {
						statMap[v.opType][0] += v.numOps
						statMap[v.opType][1] += v.timeMicros
						statMap[v.opType][2]++
					} else {
						statMap[v.opType] = []int{v.numOps, v.timeMicros, 1}
					}
				}
				stats = []FuncResult{}
				td := [][]string{
					{"Operation", "Per Second", "Avg Speed (us)"},
				}
				td = append(td, tableRow(statMap["insert"], *bencher.config.NumInsertWorkers, "Insert"))
				td = append(td, tableRow(statMap["id_read"], *bencher.config.NumIDReadWorkers, "Reads by _id"))
				td = append(td, tableRow(statMap["aggregation"], *bencher.config.NumAggregationWorkers, "Aggregations"))
				td = append(td, tableRow(statMap["update"], *bencher.config.NumUpdateWorkers, "Updates"))
				boxedTable, _ := pterm.DefaultTable.WithHasHeader().WithData(td).WithBoxed().Srender()
				area.Update(boxedTable)
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
		log.Printf("This instance is the primary, resetting the collections")
		bencher.IsPrimary = true
	} else {
		_, err := bencher.BencherInstanceCollection().InsertOne(context.Background(), &bencher)
		if err != nil {
			return err
		}
		log.Printf("Other primary exists, just starting workers")
		bencher.IsPrimary = false
	}

	if bencher.IsPrimary {
		err = bencher.MetadataMongoClient.Database(MetadataDatabase).Drop(bencher.ctx)
		if err != nil {
			return err
		}
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
	bencher.makePrimaryClient()
	bencher.makeSecondaryClient()
	bencher.makeMetadataClient()
	err := bencher.MetadataMongoClient.Database(BenchDatabase).Drop(bencher.ctx)
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
		bencher.insertWorkerMap[insertWorker.WorkerIndex] = insertWorker
	}

	for i := 0; i < *bencher.config.NumIDReadWorkers; i++ {
		StartIDReadWorker(bencher)
	}
	for i := 0; i < *bencher.config.NumUpdateWorkers; i++ {
		StartUpdateWorker(bencher)
	}
	for i := 0; i < *bencher.config.NumAggregationWorkers; i++ {
		StartAggregationWorker(bencher)
	}
	go bencher.StatWorker()

	time.Sleep(10 * time.Minute)
}
