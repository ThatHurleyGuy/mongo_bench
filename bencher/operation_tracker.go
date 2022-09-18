package bencher

import (
	"fmt"
	"time"
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

	go tracker.ControlThread()

	return tracker
}

func (tracker *OperationTracker) StartBackgroundThread() {
	tracker.controlChannel <- "start"
}

func (tracker *OperationTracker) StopBackgroundThread() {
	tracker.controlChannel <- "stop"
}

func (tracker *OperationTracker) ControlThread() {
	backgroundControlChannel := make(chan string)
	go tracker.BackgroundThread(backgroundControlChannel)
	stopped := false
	for {
		select {
		case message := <-tracker.controlChannel:
			switch message {
			case "stop":
				if !stopped {
					backgroundControlChannel <- "stop"
					stopped = true
				}
			case "start":
				if stopped {
					go tracker.BackgroundThread(backgroundControlChannel)
					stopped = false
				}
			}
		}
	}
}
func (tracker *OperationTracker) BackgroundThread(backgroundControlChannel chan string) {
	ticker := time.NewTicker(time.Duration(*tracker.bencher.config.StatTickSpeedMillis) * time.Millisecond)
	numOps := 0
	totalTimeMicros := 0
	errors := []string{}
	lastTick := time.Now()

	for {
		select {
		case <-backgroundControlChannel:
			return
		case <-ticker.C:
			elapsed := time.Since(lastTick)
			tracker.bencher.returnChannel <- &StatResult{
				totalElapsedMs: int(elapsed.Milliseconds()),
				numOps:         numOps,
				timeMicros:     totalTimeMicros,
				opType:         tracker.OpType,
				errors:         errors,
			}
			numOps = 0
			totalTimeMicros = 0
			errors = []string{}
			lastTick = time.Now()
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
