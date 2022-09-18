package bencher

type FifoQueue struct {
	maxSize    int
	oldest     *StatResult
	mostrecent *StatResult
}

func (q *FifoQueue) Clear() {
	q.oldest = q.mostrecent
	q.mostrecent = nil
}
func (q *FifoQueue) Add(item *StatResult) {
	q.mostrecent = item
}

func (q *FifoQueue) Oldest() *StatResult {
	return q.oldest
}
func (q *FifoQueue) MostRecent() *StatResult {
	return q.mostrecent
}
