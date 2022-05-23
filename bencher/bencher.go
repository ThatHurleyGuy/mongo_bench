package bencher

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/pterm/pterm"
	"github.com/thathurleyguy/gladio/cmd/config"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
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

type Bencher struct {
	ctx                   context.Context
	config                *config.Config
	workerId              int
	returnChannel         chan FuncResult
	workerMap             map[int]*InsertWorker
	numInsertWorkers      int
	numIDReadWorkers      int
	numAggregationWorkers int
	statTickSpeedMillis   int
	database              string
	collection            string
}

type FuncResult struct {
	numOps     int
	timeMicros int
	opType     string
}

func NewBencher(ctx context.Context, config *config.Config) *Bencher {
	inputChannel := make(chan FuncResult)
	bencher := &Bencher{
		ctx:                   ctx,
		config:                config,
		returnChannel:         inputChannel,
		workerMap:             map[int]*InsertWorker{},
		numInsertWorkers:      2,
		numIDReadWorkers:      2,
		numAggregationWorkers: 1,
		statTickSpeedMillis:   100,
		database:              "mongo_bench",
		collection:            "transactions",
	}
	return bencher
}

func (bencher *Bencher) Collection() *mongo.Collection {
	return bencher.config.MongoClient.Database(bencher.database).Collection(bencher.collection)
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
				td = append(td, tableRow(statMap["insert"], bencher.numInsertWorkers, "Insert"))
				td = append(td, tableRow(statMap["id_read"], bencher.numIDReadWorkers, "ID Reads"))
				td = append(td, tableRow(statMap["aggregation"], bencher.numAggregationWorkers, "Aggregations"))
				boxedTable, _ := pterm.DefaultTable.WithHasHeader().WithData(td).WithBoxed().Srender()
				area.Update(boxedTable)
			}
		}
	}
}

func (bencher *Bencher) Start() {
	collection := bencher.Collection()
	err := collection.Database().Drop(bencher.ctx)
	if err != nil {
		fmt.Println("Error dropping DB: ", err)
	} else {
		fmt.Println("Dropped database")
	}
	index := mongo.IndexModel{
		Keys: bson.D{{Key: "createdat", Value: -1}, {Key: "category", Value: 1}},
	}
	_, err = collection.Indexes().CreateOne(bencher.ctx, index)
	if err != nil {
		log.Fatal("Error creating index: ", err)
	} else {
		fmt.Println("Created indexes")
	}

	for i := 0; i < bencher.numInsertWorkers; i++ {
		insertWorker := StartInsertWorker(bencher, i)
		bencher.workerMap[i] = insertWorker
	}

	for i := 0; i < bencher.numIDReadWorkers; i++ {
		StartIDReadWorker(bencher)
	}
	for i := 0; i < bencher.numAggregationWorkers; i++ {
		StartAggregationWorker(bencher)
	}
	go bencher.StatWorker()

	time.Sleep(10 * time.Minute)
}
