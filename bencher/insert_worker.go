package bencher

import (
	"log"
	"math/rand"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type InsertWorkerPool struct {
	bencher *BencherInstance
}

// Each worker will get a serial index to help ensure uniqueness across inserts
type InsertWorker struct {
	WorkerIndex   int `bson:"workerIndex"`
	LastId        int `bson:"lastId"`
	CurrentOffset int `bson:"currentOffset"`

	bencher          *BencherInstance
	OperationTracker *OperationTracker
	wg               sync.WaitGroup
}

func (pool *InsertWorkerPool) Initialize() OperationWorker {
	workerCollection := pool.bencher.InsertWorkerCollection()

	worker := &InsertWorker{
		LastId:  0,
		bencher: pool.bencher,
		wg:      sync.WaitGroup{},
	}
	for {
		numWorkers, err := workerCollection.CountDocuments(pool.bencher.ctx, bson.M{})
		if err != nil {
			log.Fatal("Error getting insert workers: ", err)
		}

		worker.WorkerIndex = int(numWorkers) + 1
		_, err = workerCollection.InsertOne(pool.bencher.ctx, &worker)
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
		worker.bencher.insertWorkers = append(worker.bencher.insertWorkers, worker)
		return worker
	}
}

func (worker *InsertWorker) insertIntoCollection(collection *mongo.Collection, txn *Transaction) error {
	defer worker.wg.Done()
	_, insertErr := collection.InsertOne(worker.bencher.ctx, txn)
	if insertErr != nil {
		return insertErr
	}
	return nil
}

func (worker *InsertWorker) Perform() error {
	txn := Transaction{
		ID:        int64(worker.LastId + 1 + worker.CurrentOffset),
		Amount:    rand.Intn(10000),
		Category:  RandomTransactionCategory(),
		CreatedAt: time.Now(),
	}
	worker.wg.Add(1)
	err := worker.insertIntoCollection(worker.bencher.PrimaryCollection(), &txn)
	if err != nil {
		return err
	}
	if worker.bencher.SecondaryCollection() != nil {
		worker.wg.Add(1)
		err := worker.insertIntoCollection(worker.bencher.SecondaryCollection(), &txn)
		if err != nil {
			return err
		}
	}
	worker.wg.Wait()

	worker.LastId++
	return nil
}
