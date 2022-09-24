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
	bencher                      *BencherInstance
	workerTracker                map[string][]*OperationTracker
	lastCalibrationWorkerStats   map[string]*OperationWorkerStats
	recentCalibrationWorkerStats map[string]*OperationWorkerStats
	rollingStatWindow            map[string]*FifoQueue
	workerPools                  map[string]OperationPool
	workerCounts                 map[string]int
	returnChannel                chan *OperationWorkerStats
}

func (manager *WorkerManager) AddPool(optype string, pool OperationPool) {
	manager.workerPools[optype] = pool
	manager.workerCounts[optype] = 1
	manager.rollingStatWindow[optype] = NewQueue()
}

func NewWorkerManager(bencher *BencherInstance) *WorkerManager {
	return &WorkerManager{
		bencher:                      bencher,
		workerTracker:                make(map[string][]*OperationTracker),
		lastCalibrationWorkerStats:   make(map[string]*OperationWorkerStats),
		recentCalibrationWorkerStats: make(map[string]*OperationWorkerStats),
		rollingStatWindow:            make(map[string]*FifoQueue),
		workerPools:                  make(map[string]OperationPool),
		workerCounts:                 make(map[string]int),
		returnChannel:                make(chan *OperationWorkerStats),
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

				if *manager.bencher.config.AutoScale {
					manager.scaleWorkers()
				}

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
					manager.recentCalibrationWorkerStats[optype] = statWindow
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

func (manager *WorkerManager) scaleWorkers() {
	lastScaled := "insert"
	nextScaled := "insert"
	lastScaleBad := false
	mostRecent := manager.recentCalibrationWorkerStats[lastScaled]
	oldest := manager.lastCalibrationWorkerStats[lastScaled]

	if mostRecent != nil && oldest != nil {
		wasScaleUp := mostRecent.numWorkers > oldest.numWorkers
		wasScaleDown := mostRecent.numWorkers < oldest.numWorkers

		// If we scaled up and the number of queries did not grow with the rate of workers
		for optype, otherOpOldest := range manager.lastCalibrationWorkerStats {
			otherOpMostRecent := manager.recentCalibrationWorkerStats[optype]
			oldrate := float64(otherOpOldest.numOps) / float64(otherOpOldest.totalElapsedMs)
			newrate := float64(otherOpMostRecent.numOps) / float64(otherOpMostRecent.totalElapsedMs)
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

	for optype := range manager.workerCounts {
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
		manager.lastCalibrationWorkerStats[optype] = manager.recentCalibrationWorkerStats[optype]
	}
}
