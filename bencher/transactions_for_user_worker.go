package bencher

import (
	"math/rand"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type TransactionsForUserWorkerPool struct {
	bencher *BencherInstance
}

type TransactionsForUserWorker struct {
	bencher *BencherInstance
}

func (pool *TransactionsForUserWorkerPool) Initialize() OperationWorker {
	return &TransactionsForUserWorker{
		bencher: pool.bencher,
	}
}

func (worker *TransactionsForUserWorker) Perform() error {
	collection := worker.bencher.PrimaryCollectionSecondaryRead()
	userId := rand.Int31n(int32(NumUsers))
	limit := int64(50)
	opts := options.FindOptions{Limit: &limit, Sort: bson.M{"created_at": -1}}
	cursor, err := collection.Find(worker.bencher.ctx, bson.M{"user_id": userId}, &opts)
	if err != nil {
		return err
	}
	var results []bson.M
	if err = cursor.All(worker.bencher.ctx, &results); err != nil {
		return err
	}
	return nil
}

func (worker *TransactionsForUserWorker) Save() {
}
