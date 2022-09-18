package bencher

import (
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type UpdateWorker struct {
	bencher          *BencherInstance
	OperationTracker *OperationTracker
}

func StartUpdateWorker(bencher *BencherInstance) *UpdateWorker {
	updateWorker := &UpdateWorker{
		bencher: bencher,
	}
	updateWorker.Start()
	return updateWorker
}

func (updateWorker *UpdateWorker) updateDocument(collection *mongo.Collection, filter bson.M, update bson.M, wg *sync.WaitGroup) error {
	defer wg.Done()
	_, err := collection.UpdateOne(updateWorker.bencher.ctx, filter, update)
	if err != nil {
		return err
	}
	return nil
}

func (updateWorker *UpdateWorker) Start() {
	// primaryCollection := updateWorker.bencher.PrimaryCollection()
	// secondaryCollection := updateWorker.bencher.SecondaryCollection()
	// var wg sync.WaitGroup
	// op := func() error {
	// 	insertWorker := updateWorker.bencher.RandomInsertWorker()
	// 	if insertWorker.LastId == 0 {
	// 		time.Sleep(1 * time.Second)
	// 	} else {
	// 		newAmount := rand.Intn(10000)
	// 		docId := insertWorker.LastId + 1 + (insertWorker.WorkerIndex * 100_000_000_000)
	// 		filter := bson.M{"_id": docId}
	// 		update := bson.M{"$set": bson.M{"amount": newAmount}}
	// 		wg.Add(1)
	// 		err := updateWorker.updateDocument(primaryCollection, filter, update, &wg)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		if secondaryCollection != nil {
	// 			// wg.Add(1)
	// 			err := updateWorker.updateDocument(secondaryCollection, filter, update, &wg)
	// 			if err != nil {
	// 				return err
	// 			}
	// 		}
	// 		wg.Wait()
	// 	}
	// 	// TODO error
	// 	return nil
	// }
	// updateWorker.OperationTracker = NewOperationTracker(updateWorker.bencher, "update", op)
}
