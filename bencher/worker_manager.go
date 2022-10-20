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

func (o *OperationWorkerStats) Copy() *OperationWorkerStats {
	copiedErrors := []string{}
	copy(copiedErrors, o.errors)
	return &OperationWorkerStats{
		numWorkers:     o.numWorkers,
		totalElapsedMs: o.totalElapsedMs,
		numOps:         o.numOps,
		latencyMicros:  o.latencyMicros,
		opType:         o.opType,
		errors:         copiedErrors,
	}
}

type WorkerManager struct {
	bencher           *BencherInstance
	workerTracker     map[string][]*OperationTracker
	rollingStatWindow map[string]*FifoQueue
	workerPools       map[string]OperationPool
	workerCounts      map[string]int
	returnChannel     chan *OperationWorkerStats
}

func (manager *WorkerManager) AddPool(optype string, pool OperationPool) {
	manager.workerPools[optype] = pool
	manager.workerCounts[optype] = 1
	manager.rollingStatWindow[optype] = NewQueue()
}

func NewWorkerManager(bencher *BencherInstance) *WorkerManager {
	return &WorkerManager{
		bencher:           bencher,
		workerTracker:     make(map[string][]*OperationTracker),
		rollingStatWindow: make(map[string]*FifoQueue),
		workerPools:       make(map[string]OperationPool),
		workerCounts:      make(map[string]int),
		returnChannel:     make(chan *OperationWorkerStats),
	}
}

func tableRow(stats *OperationWorkerStats, statType string) []string {
	if stats == nil {
		return []string{statType, fmt.Sprint(0), fmt.Sprint(0), fmt.Sprint(map[string]int{})}
	}

	avgSpeed := 0
	perSecond := 0
	if stats.numOps > 0 {
		avgSpeed = stats.latencyMicros / stats.numOps
	}
	if stats.totalElapsedMs > 0 {
		// TODO: This seems like it's only ever increasing with number of workers and always increases
		perSecond = int(1000 * float64(stats.numOps) / float64(float64(stats.totalElapsedMs)))
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
	return []string{statType, p.Sprintf("%d", stats.numWorkers), p.Sprintf("%d", perSecond), p.Sprintf("%d", avgSpeed), fmt.Sprint(groupedErrors)}
}

func (manager *WorkerManager) Run() {
	go func() {
		ticker := time.NewTicker(time.Duration(5000) * time.Millisecond)
		redrawTicker := time.NewTicker(500 * time.Millisecond)
		for optype, count := range manager.workerCounts {
			pool := manager.workerPools[optype]
			for i := 0; i < count; i++ {
				tracker := NewOperationTracker(manager.bencher, optype, pool.Initialize())
				manager.workerTracker[optype] = []*OperationTracker{tracker}
			}
		}
		lastTick := time.Now()

		stats := []*OperationWorkerStats{}
		area, err := pterm.DefaultArea.Start()
		if err != nil {
			log.Fatal("Error setting up output area: ", err)
		}
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
			case <-redrawTicker.C:
				elapsed := time.Since(lastTick)

				statMap := map[string]*OperationWorkerStats{}
				for _, v := range stats {
					stat, ok := statMap[v.opType]
					if ok {
						stat.numOps += v.numOps
						stat.latencyMicros += v.latencyMicros
						stat.errors = append(stat.errors, v.errors...)
					} else {
						statMap[v.opType] = v.Copy()
					}
				}
				stats = []*OperationWorkerStats{}
				for optype, stats := range statMap {
					stats.totalElapsedMs = int(elapsed.Milliseconds())

					numWorkers := manager.workerCounts[optype]
					statWindow := stats.Copy()
					statWindow.numWorkers = numWorkers
					manager.rollingStatWindow[optype].Add(statWindow)
				}

				td := [][]string{
					{"Operation", "# Goroutines", "Per Second", "Avg Latency (us)", "Errors"},
				}
				for optype, window := range manager.rollingStatWindow {
					var sumWindowStats *OperationWorkerStats
					for _, statWindow := range window.All() {
						if sumWindowStats == nil {
							sumWindowStats = statWindow.Copy()
						} else {
							sumWindowStats.numOps += statWindow.numOps
							sumWindowStats.latencyMicros += statWindow.latencyMicros
							sumWindowStats.totalElapsedMs += statWindow.totalElapsedMs
							// Reset errors list properly
							sumWindowStats.errors = append(sumWindowStats.errors, statWindow.errors...)
							sumWindowStats.numWorkers = statWindow.numWorkers
						}
					}

					td = append(td, tableRow(sumWindowStats, optype))
				}
				sort.Slice(td, func(i, j int) bool {
					if i == 0 {
						return true
					} else {
						return strings.Compare(td[i][0], td[j][0]) < 0
					}
				})
				boxedTable, _ := pterm.DefaultTable.WithHasHeader().WithData(td).WithBoxed().WithRightAlignment(true).Srender()
				area.Update(boxedTable)
				lastTick = time.Now()
			}
		}
	}()
}
