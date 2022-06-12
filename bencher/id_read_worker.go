package bencher

import (
	"log"
	"math/rand"
	"time"

	"github.com/pterm/pterm"
	"go.mongodb.org/mongo-driver/bson"
)

type IDReadWorker struct {
	bencher *Bencher
}

func StartIDReadWorker(bencher *Bencher) *IDReadWorker {
	worker := &IDReadWorker{
		bencher: bencher,
	}
	go worker.Start()
	return worker
}

func (worker *IDReadWorker) Start() {
	ticker := time.NewTicker(time.Duration(*worker.bencher.config.StatTickSpeedMillis) * time.Millisecond)
	numOps := 0
	totalTimeMicros := 0
	collection := worker.bencher.PrimaryCollection()

	for {
		select {
		case <-ticker.C:
			worker.bencher.returnChannel <- FuncResult{
				numOps:     numOps,
				timeMicros: totalTimeMicros,
				opType:     "id_read",
			}
			numOps = 0
			totalTimeMicros = 0
		default:
			start := time.Now()
			insertWorker := worker.bencher.RandomInsertWorker()
			if insertWorker.LastId == 0 {
				pterm.Printfln("Waiting for insert worker to start before reading....")
				time.Sleep(1 * time.Second)
			} else {
				docId := rand.Intn(insertWorker.LastId) + 1 + (insertWorker.WorkerIndex * 100_000_000_000)
				doc := collection.FindOne(worker.bencher.ctx, bson.M{"_id": docId})
				tran := &Transaction{}
				err := doc.Decode(tran)
				if err != nil {
					log.Fatal("Bad find...", doc.Err())
				}
				totalTimeMicros += int(time.Since(start).Microseconds())
				numOps++
			}
		}
	}
}
