package bencher

import (
	"fmt"
	"log"
	"sort"
	"strings"
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
					manager.scaleWorkers()
				}
				statMap = map[string]*OperationWorkerStats{}
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
				}
				stats = []*OperationWorkerStats{}
				td := [][]string{
					{"Operation", "# Goroutines", "Per Second", "Avg Latency (us)", "Errors"},
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
				sort.Slice(td, func(i, j int) bool {
					if i == 0 {
						return true
					} else {
						return strings.Compare(td[i][0], td[j][0]) < 0
					}
				})
				boxedTable, _ := pterm.DefaultTable.WithHasHeader().WithData(td).WithBoxed().WithRightAlignment(true).Srender()
				pterm.Print(boxedTable)
				// area.Update(boxedTable)
			}
		}
	}()
}

func (manager *WorkerManager) scaleWorkers() {
	lastScaled := "insert"
	nextScaled := "insert"
	lastScaleBad := false
	lastScaledQueue := manager.workerStatQueues[lastScaled]
	mostRecent := lastScaledQueue.MostRecent()
	oldest := lastScaledQueue.Oldest()

	if mostRecent != nil && oldest != nil {
		wasScaleUp := mostRecent.numWorkers > oldest.numWorkers
		wasScaleDown := mostRecent.numWorkers < oldest.numWorkers

		// If we scaled up and the number of queries did not grow with the rate of workers
		for optype, queue := range manager.workerStatQueues {
			otherOpMostRecent := queue.MostRecent()
			otherOpOldest := queue.Oldest()
			oldrate := float64(otherOpOldest.numWorkers) * float64(otherOpOldest.numOps) / float64(otherOpOldest.totalElapsedMs)
			newrate := float64(otherOpMostRecent.numWorkers) * float64(otherOpMostRecent.numOps) / float64(otherOpMostRecent.totalElapsedMs)
			ratio := newrate / oldrate
			scaleRatio := float64(otherOpMostRecent.numWorkers) / float64(otherOpOldest.numWorkers)

			pterm.Printfln("Scale up: %+v optype: %s ratio: %d scaleratio: %d", wasScaleUp, optype, ratio, scaleRatio)
			if ratio < (0.5 * scaleRatio) {
				lastScaleBad = true
				pterm.Printfln("Last scale up made things worse for %s, scaling %s workers back down to %d", optype, lastScaled, mostRecent.numWorkers-1)
				if wasScaleUp {
					manager.workerCounts[lastScaled]--
				} else if wasScaleDown {
					manager.workerCounts[lastScaled]++
				}
				break
			}
		}
	}

	if !lastScaleBad {
		manager.workerCounts[nextScaled]++
	}

	for optype, queue := range manager.workerStatQueues {
		newCount := manager.workerCounts[optype]
		trackers := manager.workerTracker[optype]
		if manager.workerCounts[optype] > len(trackers) {
			pool := manager.workerPools[optype]
			tracker := NewOperationTracker(manager.bencher, optype, pool.Initialize())
			trackers = append(trackers, tracker)
		}
		manager.workerTracker[optype] = trackers
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
