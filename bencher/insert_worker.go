package bencher

import (
	"log"
	"math/rand"
	"time"

	"github.com/pterm/pterm"
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
}

func (pool *InsertWorkerPool) Initialize() OperationWorker {
	workerCollection := pool.bencher.InsertWorkerCollection()

	worker := &InsertWorker{
		LastId:  0,
		bencher: pool.bencher,
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
		worker.bencher.allInsertWorkers = append(worker.bencher.allInsertWorkers, worker)
		return worker
	}
}

func RandomTransactionCategory() string {
	index := rand.Intn(len(TransactionCategories))
	return TransactionCategories[index]
}

func (worker *InsertWorker) insertIntoCollection(collection *mongo.Collection, txn *Transaction) error {
	_, insertErr := collection.InsertOne(worker.bencher.ctx, txn)
	if insertErr != nil {
		return insertErr
	}
	return nil
}

func (worker *InsertWorker) Perform() error {
	txnId := int64(worker.LastId + 1 + worker.CurrentOffset)
	userId := txnId % NumUsers
	txn := Transaction{
		ID:        txnId,
		UserID:    userId,
		Amount:    rand.Intn(10000),
		Category:  RandomTransactionCategory(),
		CreatedAt: time.Now(),
	}
	err := worker.insertIntoCollection(worker.bencher.PrimaryCollection(), &txn)
	if err != nil {
		return err
	}

	worker.LastId++
	return nil
}

func (worker *InsertWorker) Save() {
	workerCollection := worker.bencher.InsertWorkerCollection()
	_, err := workerCollection.UpdateOne(worker.bencher.ctx, bson.M{"workerIndex": worker.WorkerIndex}, bson.M{"$set": bson.M{"lastId": worker.LastId}})
	if err != nil {
		pterm.Printfln("Failed to update insert worker")
	}
}
