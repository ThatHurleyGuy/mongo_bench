package bencher

type SecondaryNodeIDReadWorker struct {
	bencher          *BencherInstance
	OperationTracker *OperationTracker
}

func StartSecondaryNodeIDReadWorker(bencher *BencherInstance) *SecondaryNodeIDReadWorker {
	worker := &SecondaryNodeIDReadWorker{
		bencher: bencher,
	}
	worker.Start()
	return worker
}

func (worker *SecondaryNodeIDReadWorker) Start() {
	// collection := worker.bencher.PrimaryCollectionSecondaryRead()
	// op := func() error {
	// 	insertWorker := worker.bencher.RandomInsertWorker()
	// 	return DoReadOp(worker.bencher.ctx, insertWorker, collection)
	// }
	// worker.OperationTracker = NewOperationTracker(worker.bencher, "secondary_node_id_read", op)
}
