package bencher

import (
	"context"
	"database/sql"
	"log"
	"math"
	"math/rand"
	"time"

	_ "github.com/lib/pq"
)

type PGTransaction struct {
	ID        int64     `db:"id"`
	UserID    int64     `db:"user_id"`
	Amount    int       `db:"amount"`
	Category  string    `db:"category"`
	Metadata  string    `db:"metadata"`
	CreatedAt time.Time `db:"created_at"`
}

type PostgresBencher struct {
	DB              *sql.DB
	RODB            *sql.DB
	ctx             context.Context
	bencherInstance *BencherInstance
}

func MakePostgresClient(ctx context.Context, connString string) (*sql.DB, error) {
	db, err := sql.Open("postgres", connString)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (bencher *PostgresBencher) Setup() error {
	log.Println("Setting up postgres database")
	var err error
	// "postgres://postgres:postgres@127.0.0.1:5432/mongo_bench?sslmode=disable"
	bencher.DB, err = MakePostgresClient(bencher.ctx, *bencher.bencherInstance.config.PrimaryURI)
	if err != nil {
		log.Fatal("Error setting up postgres database: ", err)
		return err
	}
	if bencher.bencherInstance.config.ReaderURI != nil {
		log.Println("Setting up readonly postgres database")
		bencher.RODB, err = MakePostgresClient(bencher.ctx, *bencher.bencherInstance.config.ReaderURI)
		if err != nil {
			log.Fatal("Error setting up postgres database: ", err)
			return err
		}
	} else {
		log.Println("No readonly URI set, using primary")
		bencher.RODB = bencher.DB
	}
	err = bencher.SetupDB()
	if err != nil {
		log.Fatal("Error setting up postgres database: ", err)
		return err
	}
	return nil
}

func (bencher *PostgresBencher) Close() {
	bencher.DB.Close()
}

func (bencher *PostgresBencher) SetupDB() error {
	if bencher.bencherInstance.IsPrimary {
		if *bencher.bencherInstance.config.Reset {
			log.Print("Dropping old transactions table")
			_, err := bencher.DB.Exec("DROP TABLE IF EXISTS transactions")
			if err != nil {
				return err
			}
		}

		createTable := `
    CREATE TABLE IF NOT EXISTS transactions (
      id BIGSERIAL PRIMARY KEY,
      user_id BIGINT NOT NULL,
      amount INT NOT NULL,
      category VARCHAR(255) NOT NULL,
      metadata VARCHAR(1024) NOT NULL,
      created_at TIMESTAMP NOT NULL
    )`

		_, err := bencher.DB.Exec(createTable)
		if err != nil {
			return err
		}

		createIndex1 := "CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions (user_id)"
		createIndex2 := "CREATE INDEX IF NOT EXISTS idx_transactions_created_at_category ON transactions (created_at DESC, category)"
		createIndex3 := "CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions (created_at DESC)"

		_, err = bencher.DB.Exec(createIndex1)
		if err != nil {
			return err
		}

		_, err = bencher.DB.Exec(createIndex2)
		if err != nil {
			return err
		}

		_, err = bencher.DB.Exec(createIndex3)
		if err != nil {
			return err
		}
	}
	return nil
}

func (bencher *PostgresBencher) OperationPool() []OperationPool {
	readWorkers := float64(*bencher.bencherInstance.config.NumWorkers * *bencher.bencherInstance.config.WorkerReadWriteRatio / 100.0)
	readWorkers = math.Ceil(readWorkers)
	primaryIDReadWorkers := math.Floor(readWorkers * 0.25)
	secondaryIDReadWorkers := math.Floor(readWorkers * 0.5)
	aggregateReportWorkers := primaryIDReadWorkers
	userTransactionsSecondaryWorkers := readWorkers - primaryIDReadWorkers - secondaryIDReadWorkers
	writeWorkers := float64(*bencher.bencherInstance.config.NumWorkers) - readWorkers
	insertWorkers := math.Floor(writeWorkers * 0.75)
	updateWorkers := writeWorkers - insertWorkers
	insertPool := &InsertWorkerPool{
		bencher: bencher.bencherInstance,
		workers: int(insertWorkers),
		InsertFunc: func(ctx context.Context, worker *InsertWorker) error {
			txnId := int64(worker.LastId + 1 + worker.CurrentOffset)
			userId := txnId % NumUsers
			txn := PGTransaction{
				ID:        txnId,
				UserID:    userId,
				Amount:    rand.Intn(10000),
				Category:  RandomTransactionCategory(),
				Metadata:  bencher.bencherInstance.RandomString(),
				CreatedAt: time.Now(),
			}
			_, insertErr := bencher.DB.ExecContext(ctx, "INSERT INTO transactions (id, user_id, amount, category, metadata, created_at) VALUES ($1, $2, $3, $4, $5, $6)", txn.ID, txn.UserID, txn.Amount, txn.Category, txn.Metadata, txn.CreatedAt)
			if insertErr == nil {
				worker.LastId++
			}
			return insertErr
		},
	}
	transactionsForUserPool := &SimpleWorkerPool{
		opType:  "transactions_for_user",
		workers: int(userTransactionsSecondaryWorkers),
		opFunc: func(ctx context.Context, worker *SimpleWorker) error {
			userId := rand.Int31n(int32(NumUsers))
			limit := 50
			rows, err := bencher.RODB.QueryContext(ctx, "SELECT * FROM transactions WHERE user_id = $1 ORDER BY created_at DESC LIMIT $2", userId, limit)
			if err != nil {
				return err
			}
			defer rows.Close()
			var results []PGTransaction
			for rows.Next() {
				var txn PGTransaction
				if err := rows.Scan(&txn.ID, &txn.UserID, &txn.Amount, &txn.Category, &txn.Metadata, &txn.CreatedAt); err != nil {
					return err
				}
				results = append(results, txn)
			}
			return nil
		},
	}
	idReadPool := &SimpleWorkerPool{
		opType:  "primary_read",
		workers: int(primaryIDReadWorkers),
		opFunc: func(ctx context.Context, worker *SimpleWorker) error {
			// TODO: should this be against writer?
			return DoPSQLReadOp(ctx, bencher.bencherInstance.RandomInsertWorker(), bencher.RODB)
		},
	}
	secondaryIDReadPool := &SimpleWorkerPool{
		opType:  "secondary_read",
		workers: int(secondaryIDReadWorkers),
		opFunc: func(ctx context.Context, worker *SimpleWorker) error {
			return DoPSQLReadOp(ctx, bencher.bencherInstance.RandomInsertWorker(), bencher.RODB)
		},
	}
	updateWorkerPool := &SimpleWorkerPool{
		opType:  "update",
		workers: int(updateWorkers),
		opFunc: func(ctx context.Context, worker *SimpleWorker) error {
			insertWorker := bencher.bencherInstance.RandomInsertWorker()
			if insertWorker.LastId == 0 {
				time.Sleep(1 * time.Second)
			} else {
				newAmount := rand.Intn(10000)
				docId := int64(insertWorker.LastId + 1 + (insertWorker.WorkerIndex * 100_000_000_000))
				userId := docId % NumUsers
				_, err := bencher.DB.ExecContext(ctx, "UPDATE transactions SET amount = $1 WHERE id = $2 AND user_id = $3", newAmount, docId, userId)
				if err != nil {
					return err
				}
			}
			return nil
		},
	}
	aggregationPool := &SimpleWorkerPool{
		opType:  "aggregation",
		workers: int(aggregateReportWorkers),
		opFunc: func(ctx context.Context, worker *SimpleWorker) error {
			ago := time.Now().UTC().Add(-5 * time.Second)
			rows, err := bencher.RODB.QueryContext(ctx, "SELECT category, SUM(amount) as total_amount FROM transactions WHERE created_at >= $1 GROUP BY category", ago)
			if err != nil {
				return err
			}
			defer rows.Close()
			var results []struct {
				Category    string
				TotalAmount int
			}
			for rows.Next() {
				var res struct {
					Category    string
					TotalAmount int
				}
				if err := rows.Scan(&res.Category, &res.TotalAmount); err != nil {
					return err
				}
				results = append(results, res)
			}
			return nil
		},
	}
	return []OperationPool{
		transactionsForUserPool,
		insertPool,
		idReadPool,
		secondaryIDReadPool,
		updateWorkerPool,
		aggregationPool,
	}
}

func DoPSQLReadOp(ctx context.Context, insertWorker *InsertWorker, db *sql.DB) error {
	if insertWorker.LastId == 0 {
		time.Sleep(1 * time.Second)
	} else {
		docId := int64(rand.Intn(insertWorker.LastId) + 1 + (insertWorker.WorkerIndex * 100_000_000_000))
		userId := docId % NumUsers
		row := db.QueryRowContext(ctx, "SELECT id, user_id, amount, category, created_at FROM transactions WHERE id = $1 AND user_id = $2", docId, userId)
		tran := &PGTransaction{}
		err := row.Scan(&tran.ID, &tran.UserID, &tran.Amount, &tran.Category, &tran.CreatedAt)
		return err
	}
	return nil
}
