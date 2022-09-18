package bencher

import (
	"fmt"
	"log"
	"time"

	"github.com/pterm/pterm"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

func tableRow(stats *StatResult, numWorkers int, statType string) []string {
	if stats == nil {
		return []string{statType, fmt.Sprint(0), fmt.Sprint(0), fmt.Sprint(map[string]int{})}
	}

	avgSpeed := 0
	perSecond := 0
	if stats.numOps > 0 {
		avgSpeed = stats.timeMicros / stats.numOps
	}
	if stats.timeMicros > 0 {
		perSecond = int(float64(stats.numWorkers*stats.numOps) / float64(float64(stats.timeMicros)/1_000_000))
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

func (bencher *BencherInstance) StatWorker() {
	redrawTickTime := 200
	redrawTicker := time.NewTicker(time.Duration(redrawTickTime) * time.Millisecond)
	newDrawTickTime := 5000
	newDrawTimer := time.NewTicker(time.Duration(newDrawTickTime) * time.Millisecond)
	calibrateTickTime := 2000
	calibrateTicker := time.NewTicker(time.Duration(calibrateTickTime) * time.Millisecond)
	stats := []*StatResult{}
	area, err := pterm.DefaultArea.Start()
	if err != nil {
		log.Fatal("Error setting up output area: ", err)
	}

	workerStatQueues := map[string]*FifoQueue{}
	workerStatQueues["insert"] = &FifoQueue{
		maxSize: 10,
	}
	workerStatQueues["id_read"] = &FifoQueue{
		maxSize: 10,
	}
	workerStatQueues["update"] = &FifoQueue{
		maxSize: 10,
	}
	workerStatQueues["secondary_node_id_read"] = &FifoQueue{
		maxSize: 10,
	}
	workerStatQueues["aggregation"] = &FifoQueue{
		maxSize: 10,
	}
	// TODO: clean this up
	statMap := map[string]*StatResult{}
	for {
		select {
		case result := <-bencher.returnChannel:
			stats = append(stats, result)
		case <-calibrateTicker.C:
			if *bencher.config.AutoScale {
				bencher.calibrateWorkers(workerStatQueues)
			}
		case <-newDrawTimer.C:
			statMap = map[string]*StatResult{}
			area.Stop()
			pterm.Printfln("Time: %+v", time.Now())
			area, err = pterm.DefaultArea.Start()
			if err != nil {
				log.Fatal("Error setting up output area: ", err)
			}

		case <-redrawTicker.C:
			for _, v := range stats {
				stat, ok := statMap[v.opType]
				if ok {
					stat.totalElapsedMs += v.totalElapsedMs
					stat.numOps += v.numOps
					stat.timeMicros += v.timeMicros
					stat.errors = append(stat.errors, v.errors...)
				} else {
					statMap[v.opType] = v
				}
			}
			for _, v := range statMap {
				switch v.opType {
				case "insert":
					statMap[v.opType].numWorkers = *bencher.config.NumInsertWorkers
				case "id_read":
					statMap[v.opType].numWorkers = *bencher.config.NumIDReadWorkers
				case "secondary_node_id_read":
					statMap[v.opType].numWorkers = *bencher.config.NumSecondaryIDReadWorkers
				case "update":
					statMap[v.opType].numWorkers = *bencher.config.NumUpdateWorkers
				case "aggregation":
					statMap[v.opType].numWorkers = *bencher.config.NumAggregationWorkers
				}
			}
			stats = []*StatResult{}
			td := [][]string{
				{"Operation", "# Goroutines", "Per Second", "Avg Speed (us)", "Errors"},
			}
			td = append(td, tableRow(statMap["insert"], *bencher.config.NumInsertWorkers, "Insert"))
			td = append(td, tableRow(statMap["update"], *bencher.config.NumUpdateWorkers, "Updates"))
			td = append(td, tableRow(statMap["id_read"], *bencher.config.NumIDReadWorkers, "Reads by _id"))
			td = append(td, tableRow(statMap["secondary_node_id_read"], *bencher.config.NumSecondaryIDReadWorkers, "Secondary Reads"))
			td = append(td, tableRow(statMap["aggregation"], *bencher.config.NumAggregationWorkers, "Aggregations"))
			boxedTable, _ := pterm.DefaultTable.WithHasHeader().WithData(td).WithBoxed().WithRightAlignment(true).Srender()

			for optype, statResult := range statMap {
				workerStatQueues[optype].Add(&StatResult{
					totalElapsedMs: statResult.totalElapsedMs,
					numWorkers:     statResult.numWorkers,
					numOps:         statResult.numOps,
					timeMicros:     statResult.timeMicros,
					errors:         statResult.errors,
				})
			}
			area.Update(boxedTable)
			area.Stop()
		}
	}
}
func (bencher *BencherInstance) calibrateWorkers(queues map[string]*FifoQueue) {
	for optype, queue := range queues {
		mostRecent := queue.MostRecent()
		oldest := queue.Oldest()
		ratio := 1.0
		if mostRecent != nil && oldest != nil {
			oldrate := float64(oldest.numWorkers) * float64(oldest.numOps) / float64(oldest.totalElapsedMs)
			newrate := float64(mostRecent.numWorkers) * float64(mostRecent.numOps) / float64(mostRecent.totalElapsedMs)
			ratio = newrate / oldrate
		}
		trackers := bencher.workerTracker[optype]
		newCount := 0
		if ratio < 0.95 {
			switch optype {
			case "insert":
				*bencher.config.NumInsertWorkers--
				newCount = *bencher.config.NumInsertWorkers
				if newCount <= 0 {
					*bencher.config.NumInsertWorkers = 1
				}
			case "update":
				*bencher.config.NumUpdateWorkers--
				newCount = *bencher.config.NumUpdateWorkers
				if newCount <= 0 {
					*bencher.config.NumUpdateWorkers = 1
				}
			case "id_read":
				*bencher.config.NumIDReadWorkers--
				newCount = *bencher.config.NumIDReadWorkers
				if newCount <= 0 {
					*bencher.config.NumIDReadWorkers = 1
				}
			case "secondary_node_id_read":
				*bencher.config.NumSecondaryIDReadWorkers--
				newCount = *bencher.config.NumSecondaryIDReadWorkers
				if newCount <= 0 {
					*bencher.config.NumSecondaryIDReadWorkers = 1
				}
			case "aggregation":
				*bencher.config.NumAggregationWorkers--
				newCount = *bencher.config.NumAggregationWorkers
				if newCount <= 0 {
					*bencher.config.NumAggregationWorkers = 1
				}
			}
		} else {
			switch optype {
			case "insert":
				*bencher.config.NumInsertWorkers++
				newCount = *bencher.config.NumInsertWorkers
				if newCount > len(trackers) {
					insertWorker := StartInsertWorker(bencher)
					bencher.insertWorkers = append(bencher.insertWorkers, insertWorker)
					trackers = append(trackers, insertWorker.OperationTracker)
				}
			case "update":
				*bencher.config.NumUpdateWorkers++
				newCount = *bencher.config.NumUpdateWorkers
				if newCount > len(trackers) {
					worker := StartUpdateWorker(bencher)
					trackers = append(trackers, worker.OperationTracker)
				}
			case "id_read":
				*bencher.config.NumIDReadWorkers++
				newCount = *bencher.config.NumIDReadWorkers
				if newCount > len(trackers) {
					worker := StartIDReadWorker(bencher)
					trackers = append(trackers, worker.OperationTracker)
				}
			case "secondary_node_id_read":
				*bencher.config.NumSecondaryIDReadWorkers++
				newCount = *bencher.config.NumSecondaryIDReadWorkers
				if newCount > len(trackers) {
					worker := StartSecondaryNodeIDReadWorker(bencher)
					trackers = append(trackers, worker.OperationTracker)
				}
			case "aggregation":
				*bencher.config.NumAggregationWorkers++
				newCount = *bencher.config.NumAggregationWorkers
				if newCount > len(trackers) {
					worker := StartAggregationWorker(bencher)
					trackers = append(trackers, worker.OperationTracker)
				}
			}
			bencher.workerTracker[optype] = trackers
		}
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
