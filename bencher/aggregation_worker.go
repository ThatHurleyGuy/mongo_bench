package bencher

import (
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

type AggregationWorker struct {
	bencher *BencherInstance
}

func StartAggregationWorker(bencher *BencherInstance) *AggregationWorker {
	worker := &AggregationWorker{
		bencher: bencher,
	}
	go worker.Start()
	return worker
}

func (worker *AggregationWorker) Start() {
	collection := worker.bencher.PrimaryCollection()
	op := func() error {
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
		// pipeline := mongo.Pipeline{matchStage, groupStage}
		cursor, err := collection.Aggregate(worker.bencher.ctx, []bson.M{matchStage, groupStage})
		if err != nil {
			log.Fatal("Failed aggregation: ", err)
		}
		var results []bson.M
		if err = cursor.All(worker.bencher.ctx, &results); err != nil {
			log.Fatal("Failed parsing aggregation: ", err)
		}
		// TODO error
		return nil
	}
	worker.bencher.TrackOperations("aggregation", op)
}
