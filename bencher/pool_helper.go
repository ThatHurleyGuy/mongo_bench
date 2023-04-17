package bencher

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/pterm/pterm"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type InsertWorkerPool struct {
	bencher    *BencherInstance
	workers    int
	InsertFunc func(context.Context, *InsertWorker) error
}

func (pool *InsertWorkerPool) Initialize() OperationWorker {
	workerCollection := pool.bencher.InsertWorkerCollection()

	worker := &InsertWorker{
		LastId:     0,
		bencher:    pool.bencher,
		InsertFunc: pool.InsertFunc,
	}
	for {
		numWorkers, err := workerCollection.CountDocuments(pool.bencher.ctx, bson.M{})
		if err != nil {
			log.Fatal("Error getting insert workers: ", err)
		}

		worker.WorkerIndex = int(numWorkers) + 1
		_, err = workerCollection.InsertOne(pool.bencher.ctx, worker)
		if err != nil {
			if mongo.IsDuplicateKeyError(err) {
				log.Printf("Duplicate insert worker id, sleeping a bit and trying again")
				time.Sleep(time.Duration(rand.Intn(5000)) * time.Millisecond)
				continue
			} else {
				log.Fatal("Error inserting insert worker: ", err)
			}
		}
		worker.CurrentOffset = worker.WorkerIndex * 100_000_000_000
		worker.bencher.allInsertWorkers = append(worker.bencher.allInsertWorkers, worker)
		return worker
	}
}

func (worker *InsertWorker) Save(ctx context.Context) {
	workerCollection := worker.bencher.InsertWorkerCollection()
	_, err := workerCollection.UpdateOne(ctx, bson.M{"workerIndex": worker.WorkerIndex}, bson.M{"$set": bson.M{"lastId": worker.LastId}})
	if err != nil {
		pterm.Printfln("Failed to update insert worker")
	}
}

func (pool *InsertWorkerPool) OpType() string {
	return "insert"
}

func (pool *InsertWorkerPool) NumWorkers() int {
	return pool.workers
}

// Each worker will get a serial index to help ensure uniqueness across inserts
type InsertWorker struct {
	WorkerIndex   int `bson:"workerIndex"`
	LastId        int `bson:"lastId"`
	CurrentOffset int `bson:"currentOffset"`

	bencher          *BencherInstance
	OperationTracker *OperationTracker
	InsertFunc       func(context.Context, *InsertWorker) error `bson:"-"`
}

func (worker *InsertWorker) Perform(ctx context.Context) error {
	return worker.InsertFunc(ctx, worker)
}

type SimpleWorker struct {
	opFunc func(ctx context.Context, worker *SimpleWorker) error
}

type SimpleWorkerPool struct {
	opType  string
	workers int

	opFunc func(ctx context.Context, worker *SimpleWorker) error
}

func (worker *SimpleWorker) Perform(ctx context.Context) error {
	return worker.opFunc(ctx, worker)
}

func (worker *SimpleWorker) Save(ctx context.Context) {
}

func (pool *SimpleWorkerPool) Initialize() OperationWorker {
	return &SimpleWorker{
		opFunc: pool.opFunc,
	}
}

func (pool *SimpleWorkerPool) OpType() string {
	return pool.opType
}

func (pool *SimpleWorkerPool) NumWorkers() int {
	return pool.workers
}
