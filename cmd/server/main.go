package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/joho/godotenv"
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
	ctx           context.Context
	config        *config.Config
	workerId      int
	returnChannel chan FuncResult
}

type InsertWorker struct {
	bencher  *Bencher
	workerId int
}

type FuncResult struct {
	numInserts int
	avgSpeed   int
}

func (insertWorker *InsertWorker) InsertWorkerThread() {
	ticker := time.NewTicker(1 * time.Second)
	numInserts := 0
	totalTimeMicros := 0
	collection := insertWorker.bencher.config.MongoClient.Database("gladio").Collection("games")
	doc := bson.M{"title": "World", "body": "Hello World"}

	lastId := insertWorker.workerId * 100_000_000_000

	for {
		select {
		case <-ticker.C:
			insertWorker.bencher.returnChannel <- FuncResult{
				numInserts: numInserts,
				avgSpeed:   totalTimeMicros / numInserts,
			}
			numInserts = 0
			totalTimeMicros = 0
		default:
			start := time.Now()
			lastId++
			doc["_id"] = lastId
			_, insertErr := collection.InsertOne(insertWorker.bencher.ctx, doc)
			if insertErr != nil {
				log.Fatal(insertErr)
			}
			totalTimeMicros += int(time.Since(start).Microseconds())
			numInserts++
		}
	}
}

func statThread(inputChannel chan FuncResult) {
	tickTime := 5
	ticker := time.NewTicker(time.Duration(tickTime) * time.Second)
	stats := []FuncResult{}
	for {
		select {
		case result := <-inputChannel:
			stats = append(stats, result)
		case <-ticker.C:
			if len(stats) > 0 {
				totalTime := 0
				totalInserts := 0
				for _, v := range stats {
					totalTime += v.avgSpeed
					totalInserts += v.numInserts
				}
				fmt.Printf("Inserts/sec:\t%d\n", totalInserts/tickTime)
				fmt.Printf("Avg insert:\t%dus\n", totalTime/len(stats))
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
	collection.Database().Drop(ctx)

	inputChannel := make(chan FuncResult)
	bencher := &Bencher{
		ctx:           ctx,
		config:        config,
		returnChannel: inputChannel,
	}

	numInsertWorkers := 2
	for i := 0; i < numInsertWorkers; i++ {
		insertWorker := &InsertWorker{
			bencher:  bencher,
			workerId: i,
		}
		go insertWorker.InsertWorkerThread()
	}

	go statThread(inputChannel)

	time.Sleep(10 * time.Minute)
	/*
	  List databases
	*/
}
