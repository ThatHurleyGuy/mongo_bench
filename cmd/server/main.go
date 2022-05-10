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

type FuncResult struct {
  numInserts int
  avgSpeed int
}

func insertWorkerThread(ctx context.Context, config *config.Config, returnChannel chan FuncResult) {
  ticker := time.NewTicker(1 * time.Second)
  numInserts := 0
  totalTimeMicros := 0
  collection := config.MongoClient.Database("gladio").Collection("games")
  doc := bson.M{"title": "World", "body": "Hello World"}

  for {
    select {
    case <-ticker.C:
      returnChannel <- FuncResult{
        numInserts: numInserts,
        avgSpeed: totalTimeMicros / numInserts,
      }
      numInserts = 0
      totalTimeMicros = 0
    default:
      start := time.Now()
      _, insertErr := collection.InsertOne(ctx, doc)
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
    case result  := <- inputChannel:
      stats = append(stats, result)
    case <-ticker.C:
      if len(stats) > 0 {
        totalTime := 0
        totalInserts := 0
        for _, v := range stats {
          totalTime += v.avgSpeed
          totalInserts += v.numInserts
        }
        fmt.Printf("Inserts/sec:\t%d\n", totalInserts / tickTime)
        fmt.Printf("Avg insert:\t%dus\n", totalTime / len(stats))
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

  numInsertWorkers := 2
  for i := 0; i < numInsertWorkers; i++ {
    go insertWorkerThread(ctx, config, inputChannel)
  }

  go statThread(inputChannel)

  time.Sleep(10*time.Minute)
	/*
	  List databases
	*/
}
