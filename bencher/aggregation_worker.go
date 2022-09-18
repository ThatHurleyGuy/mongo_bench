package bencher

type AggregationWorker struct {
	bencher          *BencherInstance
	OperationTracker *OperationTracker
}

func StartAggregationWorker(bencher *BencherInstance) *AggregationWorker {
	worker := &AggregationWorker{
		bencher: bencher,
	}
	worker.Start()
	return worker
}

func (worker *AggregationWorker) Start() {
	// collection := worker.bencher.PrimaryCollection()
	// op := func() error {
	// 	ago := time.Now().UTC().Add(-5 * time.Second)
	// 	matchStage := bson.M{
	// 		"$match": bson.M{
	// 			"createdat": bson.M{"$gte": ago},
	// 		},
	// 	}
	// 	groupStage := bson.M{
	// 		"$group": bson.M{
	// 			"_id": "$category",
	// 			"total_amount": bson.M{
	// 				"$sum": "$amount",
	// 			},
	// 		},
	// 	}
	// 	// pipeline := mongo.Pipeline{matchStage, groupStage}
	// 	cursor, err := collection.Aggregate(worker.bencher.ctx, []bson.M{matchStage, groupStage})
	// 	if err != nil {
	// 		return err
	// 	}
	// 	var results []bson.M
	// 	if err = cursor.All(worker.bencher.ctx, &results); err != nil {
	// 		return err
	// 	}
	// 	return nil
	// }
	// worker.OperationTracker = NewOperationTracker(worker.bencher, "aggregation", op)
}
