package bencher

import (
	"fmt"
	"time"

	"github.com/pterm/pterm"
)

type OperationTracker struct {
	bencher   *BencherInstance
	operation MongoOp
	OpType    string

	controlChannel chan string
}

func NewOperationTracker(bencher *BencherInstance, opType string, fn MongoOp) *OperationTracker {
	tracker := &OperationTracker{
		bencher:        bencher,
		operation:      fn,
		OpType:         opType,
		controlChannel: make(chan string),
	}

	tracker.StartBackgroundThread()

	return tracker
}

func (tracker *OperationTracker) StartBackgroundThread() {
	go tracker.BackgroundThread()
}

func (tracker *OperationTracker) StopBackgroundThread() {
	tracker.controlChannel <- "stop"
}

func (tracker *OperationTracker) BackgroundThread() {
	ticker := time.NewTicker(time.Duration(*tracker.bencher.config.StatTickSpeedMillis) * time.Millisecond)
	numOps := 0
	totalTimeMicros := 0
	errors := []string{}

	for {
		select {
		case <-tracker.controlChannel:
			pterm.Printfln("Got shutdown for %s", tracker.OpType)
			tracker.bencher.returnChannel <- &StatResult{
				numOps:     numOps,
				timeMicros: totalTimeMicros,
				opType:     tracker.OpType,
				errors:     errors,
			}
			return
		case <-ticker.C:
			tracker.bencher.returnChannel <- &StatResult{
				numOps:     numOps,
				timeMicros: totalTimeMicros,
				opType:     tracker.OpType,
				errors:     errors,
			}
			numOps = 0
			totalTimeMicros = 0
			errors = []string{}
		default:
			start := time.Now()
			err := tracker.operation()
			totalTimeMicros += int(time.Since(start).Microseconds())
			if err != nil {
				errors = append(errors, fmt.Sprint(err.Error()))
			} else {
				numOps++
			}
		}
	}
}
