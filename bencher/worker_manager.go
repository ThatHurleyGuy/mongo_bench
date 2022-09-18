package bencher

import (
	"fmt"
	"log"
	"time"

	"github.com/pterm/pterm"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

type OperationWorkerStats struct {
	numWorkers     int
	totalElapsedMs int
	numOps         int
	latencyMicros  int
	opType         string
	errors         []string
}

type WorkerManager struct {
	bencher          *BencherInstance
	workerTracker    map[string][]*OperationTracker
	workerStatQueues map[string]*FifoQueue
	workerPools      map[string]OperationPool
	workerCounts     map[string]int
	returnChannel    chan *OperationWorkerStats
}

func (manager *WorkerManager) AddPool(optype string, pool OperationPool) {
	manager.workerPools[optype] = pool
	manager.workerStatQueues[optype] = &FifoQueue{}
	manager.workerCounts[optype] = 1
}

func NewWorkerManager(bencher *BencherInstance) *WorkerManager {
	return &WorkerManager{
		bencher:          bencher,
		workerTracker:    make(map[string][]*OperationTracker),
		workerStatQueues: make(map[string]*FifoQueue),
		workerPools:      make(map[string]OperationPool),
		workerCounts:     make(map[string]int),
		returnChannel:    make(chan *OperationWorkerStats),
	}
}

func tableRow(stats *OperationWorkerStats, numWorkers int, statType string) []string {
	if stats == nil {
		return []string{statType, fmt.Sprint(0), fmt.Sprint(0), fmt.Sprint(map[string]int{})}
	}

	avgSpeed := 0
	perSecond := 0
	if stats.numOps > 0 {
		avgSpeed = stats.latencyMicros / stats.numOps
	}
	if stats.latencyMicros > 0 {
		// TODO: This seems like it's only ever increasing with number of workers and always increases
		perSecond = int(float64(numWorkers*stats.numOps) / float64(float64(stats.latencyMicros)/1_000_000))
	}
	groupedErrors := map[string]int{}
	for _, v := range stats.errors {
		_, ok := groupedErrors[v]
		if ok {
			groupedErrors[v]++
		} else {
			groupedErrors[v] = 1
		}
	}
	p := message.NewPrinter(language.English)
	return []string{statType, p.Sprintf("%d", numWorkers), p.Sprintf("%d", perSecond), p.Sprintf("%d", avgSpeed), fmt.Sprint(groupedErrors)}
}

func (manager *WorkerManager) Run() {
	go func() {
		ticker := time.NewTicker(time.Duration(5000) * time.Millisecond)
		redrawTicker := time.NewTicker(200 * time.Millisecond)
		for optype, count := range manager.workerCounts {
			pool := manager.workerPools[optype]
			for i := 0; i < count; i++ {
				tracker := NewOperationTracker(manager.bencher, optype, pool.Initialize())
				manager.workerTracker[optype] = []*OperationTracker{tracker}
			}
		}

		stats := []*OperationWorkerStats{}
		area, err := pterm.DefaultArea.Start()
		if err != nil {
			log.Fatal("Error setting up output area: ", err)
		}
		statMap := map[string]*OperationWorkerStats{}
		for {
			select {
			case workerStats := <-manager.returnChannel:
				stats = append(stats, workerStats)
			case <-ticker.C:
				area.Stop()
				pterm.Printfln("Time: %+v", time.Now())
				area, err = pterm.DefaultArea.Start()
				if err != nil {
					log.Fatal("Error setting up output area: ", err)
				}

				if *manager.bencher.config.AutoScale {
					manager.calibrateWorkers()
				}
			case <-redrawTicker.C:
				for _, v := range stats {
					stat, ok := statMap[v.opType]
					if ok {
						stat.totalElapsedMs += v.totalElapsedMs
						stat.numOps += v.numOps
						stat.latencyMicros += v.latencyMicros
						stat.errors = append(stat.errors, v.errors...)
					} else {
						statMap[v.opType] = v
					}
					stats = []*OperationWorkerStats{}
					td := [][]string{
						{"Operation", "# Goroutines", "Per Second", "Avg Speed (us)", "Errors"},
					}
					for optype, stats := range statMap {
						numWorkers := manager.workerCounts[optype]
						manager.workerStatQueues[optype].Add(&OperationWorkerStats{
							numWorkers:     numWorkers,
							totalElapsedMs: stats.totalElapsedMs,
							numOps:         stats.numOps,
							latencyMicros:  stats.latencyMicros,
							errors:         stats.errors,
						})
						td = append(td, tableRow(stats, numWorkers, optype))
					}
					boxedTable, _ := pterm.DefaultTable.WithHasHeader().WithData(td).WithBoxed().WithRightAlignment(true).Srender()
					area.Update(boxedTable)
					area.Stop()
				}
			}
		}
	}()
}

func (manager *WorkerManager) calibrateWorkers() {
	for optype, queue := range manager.workerStatQueues {
		mostRecent := queue.MostRecent()
		oldest := queue.Oldest()
		ratio := 1.0
		pterm.Printfln("Got %s ratio %f", optype, ratio)
		if mostRecent != nil && oldest != nil {
			oldrate := float64(oldest.numWorkers) * float64(oldest.numOps) / float64(oldest.totalElapsedMs)
			newrate := float64(mostRecent.numWorkers) * float64(mostRecent.numOps) / float64(mostRecent.totalElapsedMs)
			ratio = newrate / oldrate
		}
		trackers := manager.workerTracker[optype]
		if ratio < 0.95 {
			// Reduce workers
			manager.workerCounts[optype]--
		} else {
			manager.workerCounts[optype]++
			if manager.workerCounts[optype] > len(trackers) {
				pool := manager.workerPools[optype]
				tracker := NewOperationTracker(manager.bencher, optype, pool.Initialize())
				trackers = append(trackers, tracker)
			}
			manager.workerTracker[optype] = trackers
		}
		newCount := manager.workerCounts[optype]
		for i := 0; i < len(trackers); i++ {
			if newCount > 0 {
				if i >= newCount {
					trackers[i].StopBackgroundThread()
				} else {
					trackers[i].StartBackgroundThread()
				}
			}
		}
		queue.Clear()
	}
}
