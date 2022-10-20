package bencher

import (
	"context"
	"math/rand"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type IDReadWorker struct {
	bencher *BencherInstance
}

type IDReadWorkerPool struct {
	bencher *BencherInstance
}

func (pool *IDReadWorkerPool) Initialize() OperationWorker {
	return &IDReadWorker{
		bencher: pool.bencher,
	}
}

func DoReadOp(ctx context.Context, insertWorker *InsertWorker, collection *mongo.Collection) error {
	if insertWorker.LastId == 0 {
		time.Sleep(1 * time.Second)
	} else {
		docId := rand.Intn(insertWorker.LastId) + 1 + (insertWorker.WorkerIndex * 100_000_000_000)
		doc := collection.FindOne(ctx, bson.M{"_id": docId})
		tran := &Transaction{}
		err := doc.Decode(tran)
		return err
	}
	return nil
}

func (worker *IDReadWorker) Perform() error {
	collection := worker.bencher.PrimaryCollection()
	insertWorker := worker.bencher.RandomInsertWorker()
	return DoReadOp(worker.bencher.ctx, insertWorker, collection)
}

func (worker *IDReadWorker) Save() {
}
