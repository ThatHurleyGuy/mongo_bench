package bencher

import (
	"fmt"
	"time"
)

type OperationPool interface {
	Initialize() OperationWorker
}

type OperationWorker interface {
	Perform() error
}

type OpResult struct {
	err     error
	latency int
}

type OperationTracker struct {
	bencher   *BencherInstance
	operation MongoOp
	worker    OperationWorker
	OpType    string

	controlChannel           chan string
	backgroundControlChannel chan string
	opChannel                chan OpResult
}

func NewOperationTracker(bencher *BencherInstance, opType string, worker OperationWorker) *OperationTracker {
	tracker := &OperationTracker{
		bencher:                  bencher,
		worker:                   worker,
		OpType:                   opType,
		controlChannel:           make(chan string),
		backgroundControlChannel: make(chan string),
		opChannel:                make(chan OpResult),
	}

	go tracker.ControlThread()
	go tracker.StatThread()

	return tracker
}

func (tracker *OperationTracker) StartBackgroundThread() {
	tracker.controlChannel <- "start"
}

func (tracker *OperationTracker) StopBackgroundThread() {
	tracker.controlChannel <- "stop"
}

func (tracker *OperationTracker) StatThread() {
	ticker := time.NewTicker(100 * time.Millisecond)
	numOps := 0
	latencyTimeMicros := 0
	errors := []string{}

	for {
		select {
		case result := <-tracker.opChannel:
			numOps++
			latencyTimeMicros += result.latency
			if result.err != nil {
				errors = append(errors, fmt.Sprint(result.err.Error()))
			}
		case <-ticker.C:
			tracker.bencher.WorkerManager.returnChannel <- &OperationWorkerStats{
				numOps:        numOps,
				latencyMicros: latencyTimeMicros,
				opType:        tracker.OpType,
				errors:        errors,
			}
			numOps = 0
			latencyTimeMicros = 0
			errors = []string{}
		}
	}
}

func (tracker *OperationTracker) ControlThread() {
	go tracker.BackgroundThread()
	stopped := false

	for {
		select {
		case message := <-tracker.controlChannel:
			switch message {
			case "stop":
				if !stopped {
					tracker.backgroundControlChannel <- "stop"
					stopped = true
				}
			case "start":
				if stopped {
					go tracker.BackgroundThread()
					stopped = false
				}
			}
		}
	}
}
func (tracker *OperationTracker) BackgroundThread() {
	for {
		select {
		case <-tracker.backgroundControlChannel:
			return
		default:
			start := time.Now()
			err := tracker.worker.Perform()
			latency := int(time.Since(start).Microseconds())
			tracker.opChannel <- OpResult{
				err:     err,
				latency: latency,
			}
		}
	}
}
