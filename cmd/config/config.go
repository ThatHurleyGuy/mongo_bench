package config

import (
	"context"
	"log"
	"os"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Config struct {
	ctx         *context.Context
	MongoClient *mongo.Client
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
		MongoClient: client,
	}
	return &config
}

func (config *Config) Close() {
	config.MongoClient.Disconnect(*config.ctx)
}
