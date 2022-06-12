package bencher

import (
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/pterm/pterm"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
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

func (updateWorker *UpdateWorker) updateDocument(collection *mongo.Collection, filter bson.M, update bson.M, wg *sync.WaitGroup) {
	defer wg.Done()
	_, err := collection.UpdateOne(updateWorker.bencher.ctx, filter, update)
	if err != nil {
		log.Fatal("Bad update...", err)
	}
}

func (updateWorker *UpdateWorker) Start() {
	ticker := time.NewTicker(time.Duration(*updateWorker.bencher.config.StatTickSpeedMillis) * time.Millisecond)
	numOps := 0
	totalTimeMicros := 0
	primaryCollection := updateWorker.bencher.PrimaryCollection()
	secondaryCollection := updateWorker.bencher.SecondaryCollection()
	var wg sync.WaitGroup

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
			insertWorker := updateWorker.bencher.RandomInsertWorker()
			if insertWorker.LastId == 0 {
				pterm.Printfln("Waiting for insert worker to start before updating....")
				time.Sleep(1 * time.Second)
			} else {
				newAmount := rand.Intn(10000)
				docId := insertWorker.LastId + 1 + (insertWorker.WorkerIndex * 100_000_000_000)
				filter := bson.M{"_id": docId}
				update := bson.M{"$set": bson.M{"amount": newAmount}}
				wg.Add(1)
				go updateWorker.updateDocument(primaryCollection, filter, update, &wg)
				if secondaryCollection != nil {
					wg.Add(1)
					go updateWorker.updateDocument(secondaryCollection, filter, update, &wg)
				}
				wg.Wait()
			}
			totalTimeMicros += int(time.Since(start).Microseconds())
			numOps++
		}
	}
}
