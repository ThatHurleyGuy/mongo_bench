package config

import (
	"context"
	"log"
	"os"
	"strconv"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Config struct {
	ctx                   *context.Context
	MongoClient           *mongo.Client
	NumInsertWorkers      int
	NumIDReadWorkers      int
	NumAggregationWorkers int
	NumUpdateWorkers      int
	StatTickSpeedMillis   int
	Database              string
	Collection            string
}

func Init(ctx context.Context) *Config {
	client, err := mongo.NewClient(options.Client().ApplyURI(os.Getenv("MONGO_URL")))
	if err != nil {
		log.Fatal(err)
	}
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	config := Config{
		MongoClient:           client,
		NumInsertWorkers:      getEnvInt("NUM_INSERT_WORKERS", 2),
		NumIDReadWorkers:      getEnvInt("NUM_ID_READ_WORKERS", 2),
		NumAggregationWorkers: getEnvInt("NUM_AGGREGATION_WORKERS", 1),
		NumUpdateWorkers:      getEnvInt("NUM_UPDATE_WORKERS", 2),
		StatTickSpeedMillis:   getEnvInt("STAT_TICK_SPEED_MILLIS", 100),
		Database:              getEnv("DATABASE", "mongo_bench"),
		Collection:            getEnv("COLLECTION", "transactions"),
	}
	return &config
}

func (config *Config) Close() {
	config.MongoClient.Disconnect(*config.ctx)
}

func getEnv(name string, defaultValue string) string {
	value, ok := os.LookupEnv(name)
	if !ok {
		return defaultValue
	}
	return value
}
func getEnvInt(name string, defaultValue int) int {
	value, ok := os.LookupEnv(name)
	if !ok {
		return defaultValue
	}
	intVal, err := strconv.Atoi(value)
	if err != nil {
		log.Fatal("Error parsing value: ", err)
	}
	return intVal
}
