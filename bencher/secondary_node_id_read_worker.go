package bencher

type SecondaryNodeIDReadWorkerPool struct {
	bencher *BencherInstance
}

type SecondaryNodeIDReadWorker struct {
	bencher *BencherInstance
}

func (pool *SecondaryNodeIDReadWorkerPool) Initialize() OperationWorker {
	return &SecondaryNodeIDReadWorker{
		bencher: pool.bencher,
	}
}

func (worker *SecondaryNodeIDReadWorker) Perform() error {
	insertWorker := worker.bencher.RandomInsertWorker()
	return DoReadOp(worker.bencher.ctx, insertWorker, worker.bencher.PrimaryCollectionSecondaryRead())
}
