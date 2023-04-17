package bencher

type FifoQueue struct {
	MaxSize int
	stats   []*OperationWorkerStats
}

func NewQueue() *FifoQueue {
	return &FifoQueue{
		MaxSize: 10,
		stats:   []*OperationWorkerStats{},
	}
}

func (q *FifoQueue) All() []*OperationWorkerStats {
	return q.stats
}

func (q *FifoQueue) Add(item *OperationWorkerStats) {
	if len(q.stats) >= q.MaxSize {
		q.stats = q.stats[1:]
	}
	q.stats = append(q.stats, item)
}

func (q *FifoQueue) Oldest() *OperationWorkerStats {
	return q.stats[0]
}
func (q *FifoQueue) MostRecent() *OperationWorkerStats {
	if len(q.stats) == 0 {
		return nil
	}
	return q.stats[len(q.stats)-1]
}
