package bencher

import (
	"log"
	"math/rand"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// Each worker will get a serial index to help ensure uniqueness across inserts
type InsertWorker struct {
	WorkerIndex int `bson:"workerIndex"`
	LastId      int `bson:"lastId"`

	bencher *BencherInstance
}

func StartInsertWorker(bencher *BencherInstance) *InsertWorker {
	workerCollection := bencher.InsertWorkerCollection()

	for {
		numWorkers, err := workerCollection.CountDocuments(bencher.ctx, bson.M{})
		if err != nil {
			log.Fatal("Error getting insert workers: ", err)
		}

		insertWorker := &InsertWorker{
			bencher:     bencher,
			WorkerIndex: int(numWorkers) + 1,
			LastId:      0,
		}
		_, err = workerCollection.InsertOne(bencher.ctx, &insertWorker)
		if err != nil {
			if mongo.IsDuplicateKeyError(err) {
				log.Printf("Duplicate insert worker id, sleeping a bit and trying again")
				time.Sleep(time.Duration(rand.Intn(5000)) * time.Millisecond)
				continue
			} else {
				log.Fatal("Error inserting insert worker: ", err)
			}
		} else {
			go insertWorker.Start()
			return insertWorker
		}
	}
}

func (insertWorker *InsertWorker) insertIntoCollection(collection *mongo.Collection, txn *Transaction, wg *sync.WaitGroup) error {
	defer wg.Done()
	_, insertErr := collection.InsertOne(insertWorker.bencher.ctx, txn)
	if insertErr != nil {
		return insertErr
	}
	return nil
}

func (insertWorker *InsertWorker) Start() {
	primaryCollection := insertWorker.bencher.PrimaryCollection()
	secondaryCollection := insertWorker.bencher.SecondaryCollection()

	workerIdOffset := insertWorker.WorkerIndex * 100_000_000_000
	var wg sync.WaitGroup

	op := func() error {
		txn := Transaction{
			ID:        int64(insertWorker.LastId + 1 + workerIdOffset),
			Amount:    rand.Intn(10000),
			Category:  RandomTransactionCategory(),
			Meta:      insertWorker.bencher.RandomString(),
			CreatedAt: time.Now(),
		}
		wg.Add(1)
		err := insertWorker.insertIntoCollection(primaryCollection, &txn, &wg)
		if err != nil {
			return err
		}
		if secondaryCollection != nil {
			wg.Add(1)
			err := insertWorker.insertIntoCollection(secondaryCollection, &txn, &wg)
			if err != nil {
				return err
			}
		}
		wg.Wait()

		insertWorker.LastId++
		return nil
	}
	insertWorker.bencher.TrackOperations("insert", op)
}
