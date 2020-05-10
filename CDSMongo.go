package main

import (
	"context"
	"strings"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/spf13/viper"
)

const (
	default_mongo_conn        = "mongodb://localhost:27017"
	default_mongo_autonomy_db = "autonomy"
)

func init() {
	viper.AutomaticEnv()
	viper.SetEnvPrefix("autonomy")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
}

type MongoClient struct {
	MongoClient *mongo.Client
	UsedDB      *mongo.Database
}

func NewMongoConnect() (*MongoClient, error) {
	m := MongoClient{}
	ctx := context.Background()
	conn := default_mongo_conn
	if len(viper.GetString("mongo.conn")) > 0 {
		conn = viper.GetString("mongo.conn")
	}
	opts := options.Client().ApplyURI(conn)
	client, err := mongo.NewClient(opts)
	if err != nil {
		return nil, err
	}
	m.MongoClient = client
	err = client.Connect(ctx)
	if err != nil {
		return &m, err
	}
	db := default_mongo_autonomy_db
	if len(viper.GetString("mongo.database")) > 0 {
		db = viper.GetString("mongo.database")
	}
	m.UsedDB = client.Database(db)
	return &m, nil
}
