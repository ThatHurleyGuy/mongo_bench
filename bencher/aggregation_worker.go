package bencher

import (
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

type AggregationWorker struct {
	bencher *Bencher
}

func StartAggregationWorker(bencher *Bencher) *AggregationWorker {
	worker := &AggregationWorker{
		bencher: bencher,
	}
	go worker.Start()
	return worker
}

func (worker *AggregationWorker) Start() {
	ticker := time.NewTicker(time.Duration(worker.bencher.config.StatTickSpeedMillis) * time.Millisecond)
	numOps := 0
	totalTimeMicros := 0
	collection := worker.bencher.Collection()

	for {
		select {
		case <-ticker.C:
			worker.bencher.returnChannel <- FuncResult{
				numOps:     numOps,
				timeMicros: totalTimeMicros,
				opType:     "aggregation",
			}
			numOps = 0
			totalTimeMicros = 0
		default:
			start := time.Now()
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
			totalTimeMicros += int(time.Since(start).Microseconds())
			numOps++
		}
	}
}
