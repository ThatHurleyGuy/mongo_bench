package bencher

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/pterm/pterm"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

type Transaction struct {
	ID        int64     `bson:"_id,omitempty"`
	UserID    int64     `bson:"user_id"`
	Amount    int       `bson:"amount,omitempty"`
	Category  string    `bson:"category,omitempty"`
	CreatedAt time.Time `bson:"created_at"`
}

type MongoBencher struct {
	MongoClient     *mongo.Client
	ctx             context.Context
	bencherInstance *BencherInstance
}

func (bencher *MongoBencher) PrimaryCollection() *mongo.Collection {
	return bencher.MongoClient.Database(BenchDatabase).Collection(BenchCollection)
}

func (bencher *MongoBencher) PrimaryCollectionSecondaryRead() *mongo.Collection {
	opts := options.Database().SetReadPreference(readpref.Secondary())
	return bencher.MongoClient.Database(BenchDatabase, opts).Collection(BenchCollection)
}

func MakeMongoClient(ctx context.Context, uri string) *mongo.Client {
	connectionString := options.Client().ApplyURI(uri)
	connectionString.SetWriteConcern(writeconcern.New(writeconcern.WMajority()))
	client, err := mongo.NewClient(connectionString)
	if err != nil {
		log.Fatal(err)
	}
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func (bencher *MongoBencher) Setup() error {
	log.Println("Setting up mongo database")
	bencher.makePrimaryClient()
	err := bencher.SetupDB(bencher.MongoClient)
	if err != nil {
		log.Fatal("Error setting up mongo database: ", err)
		return err
	}
	return nil
}

func (bencher *MongoBencher) Close() {
	bencher.MongoClient.Disconnect(bencher.ctx)
}

func (bencher *MongoBencher) SetupDB(client *mongo.Client) error {
	if bencher.bencherInstance.IsPrimary {
		if *bencher.bencherInstance.config.Reset {
			err := bencher.MongoClient.Database(BenchDatabase).Drop(bencher.ctx)
			if err != nil {
				return err
			}
		}

		indexes := []mongo.IndexModel{
			{Keys: bson.D{{Key: "user_id", Value: 1}}},
			{Keys: bson.D{{Key: "user_id", Value: "hashed"}}},
			{Keys: bson.D{{Key: "user_id", Value: 1}, {Key: "created_at", Value: -1}, {Key: "category", Value: 1}}},
			{Keys: bson.D{{Key: "created_at", Value: -1}, {Key: "category", Value: 1}}},
			{Keys: bson.D{{Key: "created_at", Value: -1}}},
		}

		_, err := client.Database(BenchDatabase).Collection(BenchCollection).Indexes().CreateMany(bencher.ctx, indexes)
		if err != nil {
			return err
		}
		if *bencher.bencherInstance.config.Sharded {
			result := client.Database("admin").RunCommand(bencher.ctx, bson.M{"enableSharding": BenchDatabase})
			if result.Err() != nil {
				pterm.Printfln("Failed to enable sharding")
				return result.Err()
			}

			result = client.Database("admin").RunCommand(bencher.ctx, bson.D{
				{Key: "shardCollection", Value: "mongo_bench.transactions"},
				{Key: "key", Value: bson.M{"user_id": "hashed"}},
			})
			if result.Err() != nil {
				pterm.Printfln("Failed to shard collection")
				return result.Err()
			}
		}
	}
	return nil
}

func (bencher *MongoBencher) OperationPool() []OperationPool {
	insertPool := &InsertWorkerPool{bencher: bencher, workers: *bencher.bencherInstance.config.NumInsertWorkers}
	transactionsForUserPool := TransactionsForUserWorkerPool(bencher, *bencher.bencherInstance.config.NumSecondaryIDReadWorkers)
	idReadPool := IDReadWorkerPool(bencher, *bencher.bencherInstance.config.NumIDReadWorkers)
	secondaryIDReadPool := SecondaryNodeIDReadWorkerPool(bencher, *bencher.bencherInstance.config.NumSecondaryIDReadWorkers)
	updateWorkerPool := UpdateWorkerPool(bencher, *bencher.bencherInstance.config.NumUpdateWorkers)
	aggregationPool := AggregationWorkerPool(bencher, *bencher.bencherInstance.config.NumAggregationWorkers)
	return []OperationPool{
		transactionsForUserPool,
		insertPool,
		idReadPool,
		secondaryIDReadPool,
		updateWorkerPool,
		aggregationPool,
	}
}

func (bencher *MongoBencher) RandomInsertWorker() *InsertWorker {
	for {
		if len(bencher.bencherInstance.allInsertWorkers) == 0 {
			time.Sleep(10 * time.Millisecond)
		} else {
			index := rand.Intn(len(bencher.bencherInstance.allInsertWorkers))
			return bencher.bencherInstance.allInsertWorkers[index]
		}
	}
}

func (bencher *MongoBencher) makePrimaryClient() *mongo.Client {
	if bencher.MongoClient == nil {
		bencher.MongoClient = MakeMongoClient(bencher.ctx, *bencher.bencherInstance.config.PrimaryURI)
	}
	return bencher.MongoClient
}

type InsertWorkerPool struct {
	bencher *MongoBencher
	workers int
}

// Each worker will get a serial index to help ensure uniqueness across inserts
type InsertWorker struct {
	WorkerIndex   int `bson:"workerIndex"`
	LastId        int `bson:"lastId"`
	CurrentOffset int `bson:"currentOffset"`

	bencher          *MongoBencher
	OperationTracker *OperationTracker
}

func (pool *InsertWorkerPool) Initialize() OperationWorker {
	workerCollection := pool.bencher.bencherInstance.InsertWorkerCollection()

	worker := &InsertWorker{
		LastId:  0,
		bencher: pool.bencher,
	}
	for {
		numWorkers, err := workerCollection.CountDocuments(pool.bencher.ctx, bson.M{})
		if err != nil {
			log.Fatal("Error getting insert workers: ", err)
		}

		worker.WorkerIndex = int(numWorkers) + 1
		_, err = workerCollection.InsertOne(pool.bencher.ctx, &worker)
		if err != nil {
			if mongo.IsDuplicateKeyError(err) {
				log.Printf("Duplicate insert worker id, sleeping a bit and trying again")
				time.Sleep(time.Duration(rand.Intn(5000)) * time.Millisecond)
				continue
			} else {
				log.Fatal("Error inserting insert worker: ", err)
			}
		}
		worker.CurrentOffset = worker.WorkerIndex * 100_000_000_000
		worker.bencher.bencherInstance.allInsertWorkers = append(worker.bencher.bencherInstance.allInsertWorkers, worker)
		return worker
	}
}

func (pool *InsertWorkerPool) OpType() string {
	return "insert"
}

func (pool *InsertWorkerPool) NumWorkers() int {
	return pool.workers
}

func RandomTransactionCategory() string {
	index := rand.Intn(len(TransactionCategories))
	return TransactionCategories[index]
}

func (worker *InsertWorker) insertIntoCollection(ctx context.Context, collection *mongo.Collection, txn *Transaction) error {
	_, insertErr := collection.InsertOne(ctx, txn)
	if insertErr != nil {
		return insertErr
	}
	return nil
}

func (worker *InsertWorker) Perform(ctx context.Context) error {
	txnId := int64(worker.LastId + 1 + worker.CurrentOffset)
	userId := txnId % NumUsers
	txn := Transaction{
		ID:        txnId,
		UserID:    userId,
		Amount:    rand.Intn(10000),
		Category:  RandomTransactionCategory(),
		CreatedAt: time.Now(),
	}
	err := worker.insertIntoCollection(ctx, worker.bencher.PrimaryCollection(), &txn)
	if err != nil {
		return err
	}

	worker.LastId++
	return nil
}

func (worker *InsertWorker) Save(ctx context.Context) {
	workerCollection := worker.bencher.bencherInstance.InsertWorkerCollection()
	_, err := workerCollection.UpdateOne(ctx, bson.M{"workerIndex": worker.WorkerIndex}, bson.M{"$set": bson.M{"lastId": worker.LastId}})
	if err != nil {
		pterm.Printfln("Failed to update insert worker")
	}
}

type SimpleWorker struct {
	bencher *MongoBencher
	opFunc  func(ctx context.Context, worker *SimpleWorker) error
}

type SimpleWorkerPool struct {
	opType  string
	workers int

	bencher *MongoBencher
	opFunc  func(ctx context.Context, worker *SimpleWorker) error
}

func (pool *SimpleWorkerPool) Initialize() OperationWorker {
	return &SimpleWorker{
		bencher: pool.bencher,
		opFunc:  pool.opFunc,
	}
}

func (pool *SimpleWorkerPool) OpType() string {
	return pool.opType
}

func (pool *SimpleWorkerPool) NumWorkers() int {
	return pool.workers
}

func DoReadOp(ctx context.Context, insertWorker *InsertWorker, collection *mongo.Collection) error {
	if insertWorker.LastId == 0 {
		time.Sleep(1 * time.Second)
	} else {
		docId := int64(rand.Intn(insertWorker.LastId) + 1 + (insertWorker.WorkerIndex * 100_000_000_000))
		userId := docId % NumUsers
		doc := collection.FindOne(ctx, bson.M{"_id": docId, "user_id": userId})
		tran := &Transaction{}
		err := doc.Decode(tran)
		return err
	}
	return nil
}

func (worker *SimpleWorker) Perform(ctx context.Context) error {
	return worker.opFunc(ctx, worker)
}

func (worker *SimpleWorker) Save(ctx context.Context) {
}

func IDReadWorkerPool(bencher *MongoBencher, workers int) *SimpleWorkerPool {
	return &SimpleWorkerPool{
		bencher: bencher,
		opType:  "primary_read",
		workers: workers,
		opFunc: func(ctx context.Context, worker *SimpleWorker) error {
			collection := worker.bencher.PrimaryCollection()
			insertWorker := worker.bencher.RandomInsertWorker()
			return DoReadOp(ctx, insertWorker, collection)
		},
	}
}

func SecondaryNodeIDReadWorkerPool(bencher *MongoBencher, workers int) *SimpleWorkerPool {
	return &SimpleWorkerPool{
		bencher: bencher,
		opType:  "secondary_read",
		workers: workers,
		opFunc: func(ctx context.Context, worker *SimpleWorker) error {
			insertWorker := worker.bencher.RandomInsertWorker()
			return DoReadOp(ctx, insertWorker, worker.bencher.PrimaryCollectionSecondaryRead())
		},
	}
}

func TransactionsForUserWorkerPool(bencher *MongoBencher, workers int) *SimpleWorkerPool {
	return &SimpleWorkerPool{
		bencher: bencher,
		opType:  "transactions_for_user",
		workers: workers,
		opFunc: func(ctx context.Context, worker *SimpleWorker) error {
			collection := worker.bencher.PrimaryCollectionSecondaryRead()
			userId := rand.Int31n(int32(NumUsers))
			limit := int64(50)
			opts := options.FindOptions{Limit: &limit, Sort: bson.M{"created_at": -1}}
			cursor, err := collection.Find(ctx, bson.M{"user_id": userId}, &opts)
			if err != nil {
				return err
			}
			var results []bson.M
			if err = cursor.All(ctx, &results); err != nil {
				return err
			}
			return nil
		},
	}
}

func UpdateWorkerPool(bencher *MongoBencher, workers int) *SimpleWorkerPool {
	return &SimpleWorkerPool{
		bencher: bencher,
		opType:  "update",
		workers: workers,
		opFunc: func(ctx context.Context, worker *SimpleWorker) error {
			insertWorker := worker.bencher.RandomInsertWorker()
			if insertWorker.LastId == 0 {
				time.Sleep(1 * time.Second)
			} else {
				newAmount := rand.Intn(10000)
				docId := int64(insertWorker.LastId + 1 + (insertWorker.WorkerIndex * 100_000_000_000))
				userId := docId % NumUsers
				filter := bson.M{"_id": docId, "user_id": userId}
				update := bson.M{"$set": bson.M{"amount": newAmount}}
				_, err := bencher.PrimaryCollection().UpdateOne(ctx, filter, update)
				if err != nil {
					return err
				}
			}
			return nil
		},
	}
}

func AggregationWorkerPool(bencher *MongoBencher, workers int) *SimpleWorkerPool {
	return &SimpleWorkerPool{
		bencher: bencher,
		opType:  "aggregation",
		workers: workers,
		opFunc: func(ctx context.Context, worker *SimpleWorker) error {
			collection := worker.bencher.PrimaryCollectionSecondaryRead()
			ago := time.Now().UTC().Add(-5 * time.Second)
			matchStage := bson.M{
				"$match": bson.M{
					"created_at": bson.M{"$gte": ago},
				},
			}
			groupStage := bson.M{
				"$group": bson.M{
					"_id": "$category",
					"total_amount": bson.M{
						"$sum": "$amount",
					},
				},
			}
			cursor, err := collection.Aggregate(ctx, []bson.M{matchStage, groupStage})
			if err != nil {
				return err
			}
			var results []bson.M
			if err = cursor.All(ctx, &results); err != nil {
				return err
			}
			return nil
		},
	}
}
