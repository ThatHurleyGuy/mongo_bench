package bencher

import (
	"fmt"
	"time"
)

type OperationPool interface {
	// TODO: Does this need to be a pointer to preserve state?
	Initialize() OperationWorker
}

type OperationWorker interface {
	Perform() error
}

type OperationTracker struct {
	bencher   *BencherInstance
	operation MongoOp
	worker    OperationWorker
	OpType    string

	controlChannel chan string
}

func NewOperationTracker(bencher *BencherInstance, opType string, worker OperationWorker) *OperationTracker {
	tracker := &OperationTracker{
		bencher:        bencher,
		worker:         worker,
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
	// Report stats every 50ms
	ticker := time.NewTicker(50 * time.Millisecond)
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
			tracker.bencher.WorkerManager.returnChannel <- &OperationWorkerStats{
				totalElapsedMs: int(elapsed.Milliseconds()),
				numOps:         numOps,
				latencyMicros:  totalTimeMicros,
				opType:         tracker.OpType,
				errors:         errors,
			}
			numOps = 0
			totalTimeMicros = 0
			errors = []string{}
			lastTick = time.Now()
		default:
			start := time.Now()
			err := tracker.worker.Perform()
			totalTimeMicros += int(time.Since(start).Microseconds())
			if err != nil {
				errors = append(errors, fmt.Sprint(err.Error()))
			} else {
				numOps++
			}
		}
	}
}
