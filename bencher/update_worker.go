package bencher

import (
	"log"
	"math/rand"
	"time"

	"github.com/pterm/pterm"
	"go.mongodb.org/mongo-driver/bson"
)

type UpdateWorker struct {
	bencher *Bencher
}

func StartUpdateWorker(bencher *Bencher) *UpdateWorker {
	updateWorker := &UpdateWorker{
		bencher: bencher,
	}
	go updateWorker.Start()
	return updateWorker
}

func (updateWorker *UpdateWorker) Start() {
	ticker := time.NewTicker(time.Duration(updateWorker.bencher.config.StatTickSpeedMillis) * time.Millisecond)
	numOps := 0
	totalTimeMicros := 0
	collection := updateWorker.bencher.Collection()

	for {
		select {
		case <-ticker.C:
			updateWorker.bencher.returnChannel <- FuncResult{
				numOps:     numOps,
				timeMicros: totalTimeMicros,
				opType:     "update",
			}
			numOps = 0
			totalTimeMicros = 0
		default:
			start := time.Now()
			workerId := rand.Intn(updateWorker.bencher.config.NumInsertWorkers)
			insertWorker := updateWorker.bencher.workerMap[workerId]
			if insertWorker.lastId == 0 {
				pterm.Printfln("Waiting for insert worker to start before updating....")
				time.Sleep(1 * time.Second)
			} else {
				docId := rand.Intn(updateWorker.bencher.workerMap[workerId].lastId) + 1 + (workerId * 100_000_000_000)
				_, err := collection.UpdateOne(updateWorker.bencher.ctx, bson.M{"_id": docId}, bson.M{"$set": bson.M{"amount": rand.Intn(10000)}})
				if err != nil {
					log.Fatal("Bad find...", err)
				}
			}
			totalTimeMicros += int(time.Since(start).Microseconds())
			numOps++
		}
	}
}
