package bencher

import (
	"math/rand"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type UpdateWorkerPool struct {
	bencher *BencherInstance
}

type UpdateWorker struct {
	bencher *BencherInstance
}

func (pool *UpdateWorkerPool) Initialize() OperationWorker {
	return &UpdateWorker{
		bencher: pool.bencher,
	}
}

func (updateWorker *UpdateWorker) updateDocument(collection *mongo.Collection, filter bson.M, update bson.M) error {
	_, err := collection.UpdateOne(updateWorker.bencher.ctx, filter, update)
	if err != nil {
		return err
	}
	return nil
}

func (worker *UpdateWorker) Perform() error {
	insertWorker := worker.bencher.RandomInsertWorker()
	if insertWorker.LastId == 0 {
		time.Sleep(1 * time.Second)
	} else {
		newAmount := rand.Intn(10000)
		docId := insertWorker.LastId + 1 + (insertWorker.WorkerIndex * 100_000_000_000)
		filter := bson.M{"_id": docId}
		update := bson.M{"$set": bson.M{"amount": newAmount}}
		err := worker.updateDocument(worker.bencher.PrimaryCollection(), filter, update)
		if err != nil {
			return err
		}
		if worker.bencher.SecondaryCollection() != nil {
			err := worker.updateDocument(worker.bencher.SecondaryCollection(), filter, update)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (worker *UpdateWorker) Save() {
}
