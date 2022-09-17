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
		perSecond = int(float64(numWorkers*stats.numOps) / float64(float64(stats.timeMicros)/1_000_000))
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
	rewriteTickTime := 500
	rewriteTicker := time.NewTicker(time.Duration(rewriteTickTime) * time.Millisecond)
	stats := []*StatResult{}
	area, err := pterm.DefaultArea.Start()
	if err != nil {
		log.Fatal("Error setting up output area: ", err)
	}

	lastStatBlock := time.Now()
	// TODO: clean this up
	statMap := map[string]*StatResult{}
	for {
		select {
		case result := <-bencher.returnChannel:
			stats = append(stats, result)
		case <-rewriteTicker.C:
			if time.Since(lastStatBlock).Seconds() > 5 {
				lastStatBlock = time.Now()
				statMap = map[string]*StatResult{}
				area.Stop()
				pterm.Printfln("Time: %+v", time.Now())
				area, err = pterm.DefaultArea.Start()
				if err != nil {
					log.Fatal("Error setting up output area: ", err)
				}
			}

			for _, v := range stats {
				_, ok := statMap[v.opType]
				if ok {
					stat := statMap[v.opType]
					stat.numOps += v.numOps
					stat.timeMicros += v.timeMicros
					stat.errors = append(stat.errors, v.errors...)
				} else {
					statMap[v.opType] = v
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
			area.Update(boxedTable)
		}
	}
}
