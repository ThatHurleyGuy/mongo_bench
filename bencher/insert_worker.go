package bencher

import (
	"log"
	"math/rand"
	"time"
)

type InsertWorker struct {
	bencher  *Bencher
	workerId int
	lastId   int
}

func StartInsertWorker(bencher *Bencher, workerId int) *InsertWorker {
	insertWorker := &InsertWorker{
		bencher:  bencher,
		workerId: workerId,
	}
	go insertWorker.Start()
	return insertWorker
}

func (insertWorker *InsertWorker) Start() {
	ticker := time.NewTicker(time.Duration(insertWorker.bencher.statTickSpeedMillis) * time.Millisecond)
	numInserts := 0
	totalTimeMicros := 0
	collection := insertWorker.bencher.Collection()

	insertWorker.lastId = 0
	workerIdOffset := insertWorker.workerId * 100_000_000_000

	for {
		select {
		case <-ticker.C:
			insertWorker.bencher.returnChannel <- FuncResult{
				numOps:     numInserts,
				timeMicros: totalTimeMicros,
				opType:     "insert",
			}
			numInserts = 0
			totalTimeMicros = 0
		default:
			start := time.Now()
			txn := Transaction{
				ID:        int64(insertWorker.lastId + 1 + workerIdOffset),
				Amount:    rand.Intn(10000),
				Category:  RandomTransactionCategory(),
				CreatedAt: time.Now(),
			}
			_, insertErr := collection.InsertOne(insertWorker.bencher.ctx, txn)
			if insertErr != nil {
				log.Fatal(insertErr)
			}
			insertWorker.lastId++
			totalTimeMicros += int(time.Since(start).Microseconds())
			numInserts++
		}
	}
}
