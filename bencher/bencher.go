package bencher

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/pterm/pterm"
	"github.com/thathurleyguy/gladio/cmd/config"
)

type Bencher struct {
	ctx                 context.Context
	config              *config.Config
	workerId            int
	returnChannel       chan FuncResult
	workerMap           map[int]*InsertWorker
	numInsertWorkers    int
	numIDReadWorkers    int
	statTickSpeedMillis int
}

type FuncResult struct {
	numOps     int
	timeMicros int
	opType     string
}

func NewBencher(ctx context.Context, config *config.Config) *Bencher {
	inputChannel := make(chan FuncResult)
	bencher := &Bencher{
		ctx:                 ctx,
		config:              config,
		returnChannel:       inputChannel,
		workerMap:           map[int]*InsertWorker{},
		numInsertWorkers:    2,
		numIDReadWorkers:    2,
		statTickSpeedMillis: 100,
	}
	return bencher
}

func tableRow(stats []int, numWorkers int, statType string) []string {
	avgSpeed := 0
	perSecond := 0
	if stats[0] > 0 {
		avgSpeed = stats[1] / stats[0]
	}
	if stats[1] > 0 {
		perSecond = int(float64(numWorkers*stats[0]) / float64(float64(stats[1])/1_000_000))
	}
	return []string{statType, fmt.Sprint(perSecond), fmt.Sprint(avgSpeed)}
}

func (bencher *Bencher) StatThread() {
	tickTime := 200
	ticker := time.NewTicker(time.Duration(tickTime) * time.Millisecond)
	stats := []FuncResult{}
	area, err := pterm.DefaultArea.Start()
	if err != nil {
		log.Fatal("Error setting up output area: ", err)
	}

	lastStatBlock := time.Now()
	statMap := map[string][]int{}
	statMap["insert"] = []int{0, 0, 0}
	statMap["id_read"] = []int{0, 0, 0}
	for {
		select {
		case result := <-bencher.returnChannel:
			stats = append(stats, result)
		case <-ticker.C:
			if time.Since(lastStatBlock).Seconds() > 10 {
				lastStatBlock = time.Now()
				statMap = map[string][]int{}
				statMap["insert"] = []int{0, 0, 0}
				statMap["id_read"] = []int{0, 0, 0}
				area.Stop()
				fmt.Println()
				area, err = pterm.DefaultArea.Start()
				if err != nil {
					log.Fatal("Error setting up output area: ", err)
				}
			}

			if len(stats) > 0 {
				for _, v := range stats {
					_, ok := statMap[v.opType]
					if ok {
						statMap[v.opType][0] += v.numOps
						statMap[v.opType][1] += v.timeMicros
						statMap[v.opType][2]++
					} else {
						statMap[v.opType] = []int{v.numOps, v.timeMicros, 1}
					}
				}
				stats = []FuncResult{}
				td := [][]string{
					{"Operation", "Per Second", "Avg Speed (us)"},
				}
				td = append(td, tableRow(statMap["insert"], bencher.numInsertWorkers, "Insert"))
				td = append(td, tableRow(statMap["id_read"], bencher.numIDReadWorkers, "ID Reads"))
				boxedTable, _ := pterm.DefaultTable.WithHasHeader().WithData(td).WithBoxed().Srender()
				area.Update(boxedTable)
			}
		}
	}
}

func (bencher *Bencher) Start() {
	collection := bencher.config.MongoClient.Database("gladio").Collection("games")
	err := collection.Database().Drop(bencher.ctx)
	if err != nil {
		fmt.Println("Error dropping DB: ", err)
	} else {
		fmt.Println("Dropped database")
	}

	for i := 0; i < bencher.numInsertWorkers; i++ {
		insertWorker := StartInsertWorker(bencher, i)
		bencher.workerMap[i] = insertWorker
	}

	for i := 0; i < bencher.numIDReadWorkers; i++ {
		StartIDReadWorker(bencher)
	}
	go bencher.StatThread()

	time.Sleep(10 * time.Minute)
}
