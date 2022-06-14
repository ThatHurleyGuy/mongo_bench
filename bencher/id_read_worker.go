package bencher

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/pterm/pterm"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type IDReadWorker struct {
	bencher *BencherInstance
}

func StartIDReadWorker(bencher *BencherInstance) *IDReadWorker {
	worker := &IDReadWorker{
		bencher: bencher,
	}
	go worker.Start()
	return worker
}

func DoRead(ctx context.Context, insertWorker *InsertWorker, collection *mongo.Collection) error {
	if insertWorker.LastId == 0 {
		pterm.Printfln("Waiting for insert worker to start before reading....")
		time.Sleep(1 * time.Second)
	} else {
		docId := rand.Intn(insertWorker.LastId) + 1 + (insertWorker.WorkerIndex * 100_000_000_000)
		doc := collection.FindOne(ctx, bson.M{"_id": docId})
		tran := &Transaction{}
		err := doc.Decode(tran)
		return err
	}
	return nil
}

func (worker *IDReadWorker) Start() {
	ticker := time.NewTicker(time.Duration(*worker.bencher.config.StatTickSpeedMillis) * time.Millisecond)
	numOps := 0
	totalTimeMicros := 0
	collection := worker.bencher.PrimaryCollection()
	errors := []string{}

	// TODO: Extract this stat tracking pattern
	for {
		select {
		case <-ticker.C:
			worker.bencher.returnChannel <- &FuncResult{
				numOps:     numOps,
				timeMicros: totalTimeMicros,
				opType:     "id_read",
				errors:     errors,
			}
			numOps = 0
			totalTimeMicros = 0
			errors = []string{}
		default:
			start := time.Now()
			err := DoRead(worker.bencher.ctx, worker.bencher.RandomInsertWorker(), collection)
			if err != nil {
				errors = append(errors, fmt.Sprint(err.Error()))
			} else {
				numOps++
			}
			totalTimeMicros += int(time.Since(start).Microseconds())
		}
	}
}
