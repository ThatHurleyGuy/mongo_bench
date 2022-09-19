package bencher

import (
	"math/rand"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type UpdateWorkerPool struct {
	bencher *BencherInstance
}

type UpdateWorker struct {
	bencher *BencherInstance
	wg      sync.WaitGroup
}

func (pool *UpdateWorkerPool) Initialize() OperationWorker {
	return &UpdateWorker{
		wg:      sync.WaitGroup{},
		bencher: pool.bencher,
	}
}

func (updateWorker *UpdateWorker) updateDocument(collection *mongo.Collection, filter bson.M, update bson.M, wg *sync.WaitGroup) error {
	defer wg.Done()
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
		worker.wg.Add(1)
		err := worker.updateDocument(worker.bencher.PrimaryCollection(), filter, update, &worker.wg)
		if err != nil {
			return err
		}
		if worker.bencher.SecondaryCollection() != nil {
			worker.wg.Add(1)
			err := worker.updateDocument(worker.bencher.SecondaryCollection(), filter, update, &worker.wg)
			if err != nil {
				return err
			}
		}
		worker.wg.Wait()
	}
	// TODO error
	return nil
}
