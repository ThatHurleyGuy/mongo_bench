package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/joho/godotenv"
	"github.com/pterm/pterm"
	"github.com/thathurleyguy/gladio/cmd/config"
	"go.mongodb.org/mongo-driver/bson"
)

func init() {
	err := godotenv.Load(".env")

	if err != nil {
		log.Fatal("Error loading .env file")
	}
}

type Bencher struct {
	ctx                 context.Context
	config              *config.Config
	workerId            int
	returnChannel       chan FuncResult
	workerMap           map[int]*InsertWorker
	numInsertWorkers    int
	numIDReadWorkers    int
	statTickSpeedMillis int
}

type InsertWorker struct {
	bencher  *Bencher
	workerId int
	lastId   int
}

type IDReadWorker struct {
	bencher *Bencher
}

type FuncResult struct {
	numOps     int
	timeMicros int
	opType     string
}

func (insertWorker *InsertWorker) InsertWorkerThread() {
	ticker := time.NewTicker(time.Duration(insertWorker.bencher.statTickSpeedMillis) * time.Millisecond)
	numInserts := 0
	totalTimeMicros := 0
	collection := insertWorker.bencher.config.MongoClient.Database("gladio").Collection("games")
	doc := bson.M{"title": "World", "body": "Hello World"}

	insertWorker.lastId = 0
	workerIdOffset := insertWorker.workerId * 100_000_000_000

	for {
		select {
		case <-ticker.C:
			insertWorker.bencher.returnChannel <- FuncResult{
				numOps:     numInserts,
				timeMicros: totalTimeMicros,
				opType:     "insert",
			}
			numInserts = 0
			totalTimeMicros = 0
		default:
			start := time.Now()
			doc["_id"] = insertWorker.lastId + 1 + workerIdOffset
			_, insertErr := collection.InsertOne(insertWorker.bencher.ctx, doc)
			if insertErr != nil {
				log.Fatal(insertErr)
			}
			insertWorker.lastId++
			totalTimeMicros += int(time.Since(start).Microseconds())
			numInserts++
		}
	}
}

func (worker *IDReadWorker) readThread() {
	ticker := time.NewTicker(time.Duration(worker.bencher.statTickSpeedMillis) * time.Millisecond)
	numOps := 0
	totalTimeMicros := 0
	collection := worker.bencher.config.MongoClient.Database("gladio").Collection("games")

	for {
		select {
		case <-ticker.C:
			worker.bencher.returnChannel <- FuncResult{
				numOps:     numOps,
				timeMicros: totalTimeMicros,
				opType:     "id_read",
			}
			numOps = 0
			totalTimeMicros = 0
		default:
			start := time.Now()
			workerId := rand.Intn(worker.bencher.numInsertWorkers)
			insertWorker := worker.bencher.workerMap[workerId]
			if insertWorker.lastId == 0 {
				pterm.Printfln("Waiting for insert worker to start before reading....")
				time.Sleep(1 * time.Second)
			} else {
				docId := rand.Intn(worker.bencher.workerMap[workerId].lastId) + 1 + (workerId * 100_000_000_000)
				doc := collection.FindOne(worker.bencher.ctx, bson.M{"_id": docId})
				if doc.Err() != nil {
					log.Fatal("Bad find...", doc.Err())
				}
				totalTimeMicros += int(time.Since(start).Microseconds())
				numOps++
			}
		}
	}
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

func (bencher *Bencher) statThread() {
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
				boxedTable, _ := pterm.DefaultTable.WithHasHeader().WithData(td).WithBoxed().Srender()
				area.Update(boxedTable)
			}
		}
	}
}

func main() {
	flag.Parse()

	fmt.Println("Main")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	config := config.Init(ctx)
	defer config.Close()

	collection := config.MongoClient.Database("gladio").Collection("games")
	err := collection.Database().Drop(ctx)
	if err != nil {
		fmt.Println("Error dropping DB: ", err)
	} else {
		fmt.Println("Dropped database")
	}

	inputChannel := make(chan FuncResult)
	bencher := &Bencher{
		ctx:                 ctx,
		config:              config,
		returnChannel:       inputChannel,
		workerMap:           map[int]*InsertWorker{},
		numInsertWorkers:    2,
		numIDReadWorkers:    2,
		statTickSpeedMillis: 100,
	}

	for i := 0; i < bencher.numInsertWorkers; i++ {
		insertWorker := &InsertWorker{
			bencher:  bencher,
			workerId: i,
		}
		bencher.workerMap[i] = insertWorker
		go insertWorker.InsertWorkerThread()
	}

	for i := 0; i < bencher.numIDReadWorkers; i++ {
		worker := &IDReadWorker{
			bencher: bencher,
		}
		go worker.readThread()
	}
	go bencher.statThread()

	time.Sleep(10 * time.Minute)
}
