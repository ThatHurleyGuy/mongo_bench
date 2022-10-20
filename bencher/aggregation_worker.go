package bencher

import (
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

type AggregationWorkerPool struct {
	bencher *BencherInstance
}

type AggregationWorker struct {
	bencher *BencherInstance
}

func (pool *AggregationWorkerPool) Initialize() OperationWorker {
	return &AggregationWorker{
		bencher: pool.bencher,
	}
}

func (worker *AggregationWorker) Perform() error {
	collection := worker.bencher.PrimaryCollectionSecondaryRead()
	ago := time.Now().UTC().Add(-5 * time.Second)
	matchStage := bson.M{
		"$match": bson.M{
			"createdat": bson.M{"$gte": ago},
		},
	}
	groupStage := bson.M{
		"$group": bson.M{
			"_id": "$category",
			"total_amount": bson.M{
				"$sum": "$amount",
			},
		},
	}
	cursor, err := collection.Aggregate(worker.bencher.ctx, []bson.M{matchStage, groupStage})
	if err != nil {
		return err
	}
	var results []bson.M
	if err = cursor.All(worker.bencher.ctx, &results); err != nil {
		return err
	}
	return nil
}

func (worker *AggregationWorker) Save() {
}
