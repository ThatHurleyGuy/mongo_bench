package bencher

import (
	"fmt"
	"time"
)

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
	ticker := time.NewTicker(time.Duration(*worker.bencher.config.StatTickSpeedMillis) * time.Millisecond)
	numOps := 0
	totalTimeMicros := 0
	collection := worker.bencher.PrimaryCollectionSecondaryRead()
	errors := []string{}

	for {
		select {
		case <-ticker.C:
			worker.bencher.returnChannel <- &FuncResult{
				numOps:     numOps,
				timeMicros: totalTimeMicros,
				opType:     "secondary_node_id_read",
				errors:     errors,
			}
			numOps = 0
			totalTimeMicros = 0
			errors = []string{}
		default:
			start := time.Now()
			err := DoRead(worker.bencher.ctx, worker.bencher.RandomInsertWorker(), collection)
			if err != nil {
				// TODO start grouping errors by type
				// log.Printf("Secondary Read Error: %+v", err)
				errors = append(errors, fmt.Sprint(err.Error()))
			} else {
				numOps++
			}
			totalTimeMicros += int(time.Since(start).Microseconds())
		}
	}
}
