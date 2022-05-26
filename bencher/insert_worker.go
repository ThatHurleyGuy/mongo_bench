package bencher

import (
	"log"
	"math/rand"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
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

func (insertWorker *InsertWorker) insertIntoCollection(collection *mongo.Collection, txn *Transaction, wg *sync.WaitGroup) {
	defer wg.Done()
	_, insertErr := collection.InsertOne(insertWorker.bencher.ctx, txn)
	if insertErr != nil {
		log.Fatal(insertErr)
	}
}

func (insertWorker *InsertWorker) Start() {
	ticker := time.NewTicker(time.Duration(insertWorker.bencher.config.StatTickSpeedMillis) * time.Millisecond)
	numInserts := 0
	totalTimeMicros := 0
	primaryCollection := insertWorker.bencher.PrimaryCollection()
	secondaryCollection := insertWorker.bencher.SecondaryCollection()

	insertWorker.lastId = 0
	workerIdOffset := insertWorker.workerId * 100_000_000_000
	var wg sync.WaitGroup

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
			wg.Add(2)
			go insertWorker.insertIntoCollection(primaryCollection, &txn, &wg)
			go insertWorker.insertIntoCollection(secondaryCollection, &txn, &wg)
			wg.Wait()

			insertWorker.lastId++
			totalTimeMicros += int(time.Since(start).Microseconds())
			numInserts++
		}
	}
}
