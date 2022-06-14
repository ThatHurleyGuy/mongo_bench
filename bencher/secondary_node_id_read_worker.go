package bencher

import (
	"log"
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
	errors := 0

	for {
		select {
		case <-ticker.C:
			worker.bencher.returnChannel <- FuncResult{
				numOps:     numOps,
				timeMicros: totalTimeMicros,
				opType:     "secondary_node_id_read",
				errors:     errors,
			}
			numOps = 0
			totalTimeMicros = 0
		default:
			start := time.Now()
			err := DoRead(worker.bencher.ctx, worker.bencher.RandomInsertWorker(), collection)
			if err != nil {
				log.Printf("Error: %+v", err)
				errors++
			} else {
				numOps++
			}
			totalTimeMicros += int(time.Since(start).Microseconds())
		}
	}
}
