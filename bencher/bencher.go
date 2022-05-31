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
)

var TransactionCategories = []string{"first_sale", "refund", "promotion"}

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
	NumInsertWorkers      *int
	NumIDReadWorkers      *int
	NumAggregationWorkers *int
	NumUpdateWorkers      *int
	StatTickSpeedMillis   *int
	Database              *string
	Collection            *string
}

type Bencher struct {
	ctx                  context.Context
	config               *Config
	returnChannel        chan FuncResult
	workerMap            map[int]*InsertWorker
	PrimaryMongoClient   *mongo.Client
	SecondaryMongoClient *mongo.Client
}

type FuncResult struct {
	numOps     int
	timeMicros int
	opType     string
}

func NewBencher(ctx context.Context, config *Config) *Bencher {
	inputChannel := make(chan FuncResult)
	bencher := &Bencher{
		ctx:           ctx,
		config:        config,
		returnChannel: inputChannel,
		workerMap:     map[int]*InsertWorker{},
	}
	return bencher
}

func (bencher *Bencher) PrimaryCollection() *mongo.Collection {
	return bencher.PrimaryMongoClient.Database(*bencher.config.Database).Collection(*bencher.config.Collection)
}

func (bencher *Bencher) SecondaryCollection() *mongo.Collection {
	if bencher.SecondaryMongoClient == nil {
		return nil
	}
	return bencher.SecondaryMongoClient.Database(*bencher.config.Database).Collection(*bencher.config.Collection)
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

func (bencher *Bencher) StatWorker() {
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
				td = append(td, tableRow(statMap["id_read"], *bencher.config.NumIDReadWorkers, "ID Reads"))
				td = append(td, tableRow(statMap["aggregation"], *bencher.config.NumAggregationWorkers, "Aggregations"))
				td = append(td, tableRow(statMap["update"], *bencher.config.NumUpdateWorkers, "Updates"))
				boxedTable, _ := pterm.DefaultTable.WithHasHeader().WithData(td).WithBoxed().Srender()
				area.Update(boxedTable)
			}
		}
	}
}

func (bencher *Bencher) SetupDB(ctx context.Context, uri string) (*mongo.Client, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatal(err)
	}
	err = client.Connect(bencher.ctx)
	if err != nil {
		log.Fatal(err)
	}
	err = client.Database(*bencher.config.Database).Drop(ctx)
	if err != nil {
		return nil, err
	}
	index := mongo.IndexModel{
		Keys: bson.D{{Key: "createdat", Value: -1}, {Key: "category", Value: 1}},
	}
	_, err = client.Database(*bencher.config.Database).Collection(*bencher.config.Collection).Indexes().CreateOne(ctx, index)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (bencher *Bencher) Close() {
	bencher.PrimaryMongoClient.Disconnect(bencher.ctx)
	if bencher.SecondaryMongoClient != nil {
		bencher.SecondaryMongoClient.Disconnect(bencher.ctx)
	}
}

func (bencher *Bencher) Start() {
	defer bencher.Close()
	var err error
	log.Println("Setting up primary")
	bencher.PrimaryMongoClient, err = bencher.SetupDB(bencher.ctx, *bencher.config.PrimaryURI)
	if err != nil {
		log.Fatal("Error setting up primary: ", err)
	}
	if *bencher.config.SecondaryURI != "" {
		log.Println("Setting up secondary")
		bencher.SecondaryMongoClient, err = bencher.SetupDB(bencher.ctx, *bencher.config.SecondaryURI)
		if err != nil {
			log.Fatal("Error reseting secondary: ", err)
		}
	}

	for i := 0; i < *bencher.config.NumInsertWorkers; i++ {
		insertWorker := StartInsertWorker(bencher, i)
		bencher.workerMap[i] = insertWorker
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
