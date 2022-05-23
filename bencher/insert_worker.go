package bencher

import (
	"log"
	"time"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
)

func init() {
	err := godotenv.Load(".env")

	if err != nil {
		log.Fatal("Error loading .env file")
	}
}

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
	go insertWorker.InsertThread()
	return insertWorker
}

func (insertWorker *InsertWorker) InsertThread() {
	ticker := time.NewTicker(time.Duration(insertWorker.bencher.statTickSpeedMillis) * time.Millisecond)
	numInserts := 0
	totalTimeMicros := 0
	collection := insertWorker.bencher.config.MongoClient.Database("gladio").Collection("games")
	doc := bson.M{"title": "World", "body": "Hello World"}

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
			doc["_id"] = insertWorker.lastId + 1 + workerIdOffset
			_, insertErr := collection.InsertOne(insertWorker.bencher.ctx, doc)
			if insertErr != nil {
				log.Fatal(insertErr)
			}
			insertWorker.lastId++
			totalTimeMicros += int(time.Since(start).Microseconds())
			numInserts++
		}
	}
}
