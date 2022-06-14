package bencher

type SecondaryNodeIDReadWorker struct {
	bencher *BencherInstance
}

func StartSecondaryNodeIDReadWorker(bencher *BencherInstance) *SecondaryNodeIDReadWorker {
	worker := &SecondaryNodeIDReadWorker{
		bencher: bencher,
	}
	go worker.Start()
	return worker
}

func (worker *SecondaryNodeIDReadWorker) Start() {
	collection := worker.bencher.PrimaryCollectionSecondaryRead()
	op := DoReadOp(worker.bencher.ctx, worker.bencher.RandomInsertWorker(), collection)
	worker.bencher.TrackOperations("secondary_node_id_read", op)
}
