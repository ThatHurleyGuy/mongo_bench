package bencher

type FifoQueue struct {
	oldest     *OperationWorkerStats
	mostrecent *OperationWorkerStats
}

func (q *FifoQueue) Clear() {
	q.oldest = q.mostrecent
	q.mostrecent = nil
}
func (q *FifoQueue) Add(item *OperationWorkerStats) {
	q.mostrecent = item
}

func (q *FifoQueue) Oldest() *OperationWorkerStats {
	return q.oldest
}
func (q *FifoQueue) MostRecent() *OperationWorkerStats {
	return q.mostrecent
}
