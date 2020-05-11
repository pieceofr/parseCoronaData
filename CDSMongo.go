package main

import (
	"context"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
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
func setIndex(c *MongoClient, collection string) error {
	cdsIndex := mongo.IndexModel{
		Keys:    bson.D{{"name", 1}, {"report_ts", 1}},
		Options: options.Index().SetUnique(true),
	}
	_, err := c.UsedDB.Collection(collection).Indexes().CreateOne(context.Background(), cdsIndex)

	if nil != err {
		fmt.Println("collection", collection, "mongodb create name and report_ts combined index with error: ", err)
		return err
	}
	return nil
}

func createCDSData(c *MongoClient, result []CDSData, collection string) error {
	data := make([]interface{}, len(result))
	for i, v := range result {
		data[i] = v
	}
	opts := options.InsertMany().SetOrdered(false)
	_, err := c.UsedDB.Collection(collection).InsertMany(context.Background(), data, opts)
	if err != nil {
		if errs, hasErr := err.(mongo.BulkWriteException); hasErr {
			if 1 == len(errs.WriteErrors) && DuplicateKeyCode == errs.WriteErrors[0].Code {
				fmt.Println(err)
				return nil
			}
		}
	}
	//fmt.Println("result of insert mongodb", res)
	return nil
}
