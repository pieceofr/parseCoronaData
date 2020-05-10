package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var USAData []CDSData

const (
	DataDir                 = "data"
	CollectionConfirmUS     = "ConfirmUS"
	CollectionConfirmTaiwan = "ConfirmTaiwan"
	DuplicateKeyCode        = 11000
)

func main() {
	//go PrintUsage()
	file, err := getDataFilePath(CDS)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	CDSHistoryToDB(file)
}

func getDataFilePath(source CovidSource) (string, error) {
	working, err := os.Getwd()
	if err != nil {
		return "", err
	}
	switch source {
	case CDS:
		path := path.Join(working, DataDir, "timeseries-byLocation.json")
		return path, nil
	default:
		return "", errors.New("no data source")
	}
	return "", nil
}

func CDSHistoryToDB(cdsFile string) {
	f, err := os.Open(cdsFile)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	parser := NewCDSParser(CDSTimeseriesLocation, "United State", "county", f)
	cnt, rawRecordCount, err := parser.ParseHistory()
	if err != nil {
		log.Println("US Data Parse Error", err)
		return
	}
	f.Close()
	log.Println("US data get:", cnt, " rawRecordCount in file:", rawRecordCount)
	client, err := NewMongoConnect()
	if err != nil {
		fmt.Println("connect to autonomy db error:", err)
		return
	}
	err = createCDSData(client, parser.Result, CollectionConfirmUS)
	if err != nil {
		fmt.Println("create US CDSData error:", err)
		return
	}
	f, err = os.Open(cdsFile)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	parser = NewCDSParser(CDSTimeseriesLocation, "Taiwan", "country", f)
	cnt, rawRecordCount, err = parser.ParseHistory()
	if err != nil {
		log.Println("Taiwan  Data Parse Error", err)
		return
	}
	f.Close()
	log.Println("Taiwan data get:", cnt, " rawRecordCount in file:", rawRecordCount)
	err = createCDSData(client, parser.Result, CollectionConfirmTaiwan)
	if err != nil {
		fmt.Println("create Taiwan CDSData error:", err)
		return
	}
	return

}

func createCDSData(c *MongoClient, result []CDSData, collection string) error {
	data := make([]interface{}, len(result))
	for i, v := range result {
		data[i] = v
	}
	cdsIndex := mongo.IndexModel{
		Keys: bson.M{
			"name":      1,
			"report_ts": 1,
		},
		Options: options.Index().SetUnique(true),
	}
	_, err := c.UsedDB.Collection(collection).Indexes().CreateOne(context.Background(), cdsIndex)

	if nil != err {
		fmt.Println("mongodb create name and report_ts combined index with error: ", err)
		return err
	}

	opts := options.InsertMany().SetOrdered(false)
	_, err = c.UsedDB.Collection(collection).InsertMany(context.Background(), data, opts)
	if err != nil {
		if errs, hasErr := err.(mongo.BulkWriteException); hasErr {
			if 1 == len(errs.WriteErrors) && DuplicateKeyCode == errs.WriteErrors[0].Code {
				fmt.Println(err)
				return nil
			}
		}
	}
	return nil
}
