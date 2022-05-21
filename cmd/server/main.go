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
	ctx              context.Context
	config           *config.Config
	workerId         int
	returnChannel    chan FuncResult
	workerMap        map[int]*InsertWorker
	numInsertWorkers int
	numIDReadWorkers int
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
	numOps   int
	avgSpeed int
	opType   string
}

func (insertWorker *InsertWorker) InsertWorkerThread() {
	ticker := time.NewTicker(1 * time.Second)
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
				numOps:   numInserts,
				avgSpeed: totalTimeMicros / numInserts,
				opType:   "insert",
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
	ticker := time.NewTicker(1 * time.Second)
	numOps := 0
	totalTimeMicros := 0
	collection := worker.bencher.config.MongoClient.Database("gladio").Collection("games")

	for {
		select {
		case <-ticker.C:
			avgSpeed := 0
			if numOps > 0 {
				avgSpeed = totalTimeMicros / numOps
			}
			worker.bencher.returnChannel <- FuncResult{
				numOps:   numOps,
				avgSpeed: avgSpeed,
				opType:   "id_read",
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

func (bencher *Bencher) statThread() {
	tickTime := 5
	ticker := time.NewTicker(time.Duration(tickTime) * time.Second)
	stats := []FuncResult{}
	for {
		select {
		case result := <-bencher.returnChannel:
			stats = append(stats, result)
		case <-ticker.C:
			if len(stats) > 0 {
				statMap := map[string][]int{}
				for _, v := range stats {
					_, ok := statMap[v.opType]
					if ok {
						statMap[v.opType][0] += v.avgSpeed
						statMap[v.opType][1] += v.numOps
						statMap[v.opType][2]++
					} else {
						statMap[v.opType] = []int{v.avgSpeed, v.numOps, 1}
					}
				}
				td := [][]string{
					{"Operation", "Per Second", "Avg Speed (us)"},
				}
				insertStats := statMap["insert"]
				td = append(td, []string{"Insert", fmt.Sprint(insertStats[1] / tickTime), fmt.Sprint(insertStats[0] / insertStats[2])})
				idReadStats := statMap["id_read"]
				td = append(td, []string{"ID Reads", fmt.Sprint(idReadStats[1] / tickTime), fmt.Sprint(idReadStats[0] / idReadStats[2])})
				boxedTable, _ := pterm.DefaultTable.WithHasHeader().WithData(td).WithBoxed().Srender()
				pterm.Println(boxedTable)
				stats = []FuncResult{}
			} else {
				fmt.Println("No stats this tick...")
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
		ctx:              ctx,
		config:           config,
		returnChannel:    inputChannel,
		workerMap:        map[int]*InsertWorker{},
		numInsertWorkers: 2,
		numIDReadWorkers: 2,
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
