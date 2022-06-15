package bencher

import (
	"context"
	"math/rand"
	"time"

	"github.com/pterm/pterm"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type IDReadWorker struct {
	bencher *BencherInstance
}

func StartIDReadWorker(bencher *BencherInstance) *IDReadWorker {
	worker := &IDReadWorker{
		bencher: bencher,
	}
	go worker.Start()
	return worker
}

func DoReadOp(ctx context.Context, insertWorker *InsertWorker, collection *mongo.Collection) error {
	if insertWorker.LastId == 0 {
		pterm.Printfln("Waiting for insert worker to start before reading....")
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

func (worker *IDReadWorker) Start() {
	collection := worker.bencher.PrimaryCollection()
	op := func() error {
		insertWorker := worker.bencher.RandomInsertWorker()
		return DoReadOp(worker.bencher.ctx, insertWorker, collection)
	}
	worker.bencher.TrackOperations("id_read", op)
}
