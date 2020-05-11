package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path"
)

var USAData []CDSData

const (
	DataDir                 = "data"
	CollectionConfirmUS     = "ConfirmUS"
	CollectionConfirmTaiwan = "ConfirmTaiwan"
	DuplicateKeyCode        = 11000
	coronaDataScraperURL    = "https://coronadatascraper.com/data.json"
)

func main() {
	//go PrintUsage()
	job := "history"
	client, err := NewMongoConnect()
	if err != nil {
		fmt.Println("connect to autonomy db error:", err)
		return
	}
	switch job {
	case "history":
		file, err := getDataFilePath(CDSTimeseriesLocationFile)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		// no earlier than 2020-04-01 1585699200
		CDSHistoryToDB(client, file, 1585699200)
	case "daily":
		file, _ := getDataFilePath(CDSDaily)
		log.Println("filepath=", file)
		err := CDSDailyUpdate(client, file)
		if err != nil {
			fmt.Println("parse daily err", err)
		}
	case "online":
		err := CDSDailyOnline(client, coronaDataScraperURL)
		if err != nil {
			fmt.Println("parse daily online err", err)
		}

	}

}

func getDataFilePath(source CovidSource) (string, error) {
	working, err := os.Getwd()
	if err != nil {
		return "", err
	}

	switch source {
	case CDSTimeseriesLocationFile:
		path := path.Join(working, DataDir, "timeseries-byLocation.json")
		return path, nil
	case CDSDaily:
		path := path.Join(working, DataDir, "dataDaily.json")
		return path, nil
	default:
		return "", errors.New("no data source")
	}
	return "", nil
}

func CDSHistoryToDB(client *MongoClient, cdsFile string, noEarlier int64) {
	f, err := os.Open(cdsFile)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	parser := NewCDSParser(CDSTimeseriesLocationFile, "United State", "county", f, "")
	cnt, rawRecordCount, err := parser.ParseHistory(noEarlier)
	if err != nil {
		log.Println("US Data Parse Error", err)
		return
	}
	f.Close()
	log.Println("US data get:", cnt, " rawRecordCount in file:", rawRecordCount)
	err = setIndex(client, CollectionConfirmUS)
	if err != nil {
		fmt.Println("set ConfirmUS index error:", err)
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
	err = setIndex(client, CollectionConfirmTaiwan)
	if err != nil {
		fmt.Println("set ConfirmTW index error:", err)
		return
	}

	parser = NewCDSParser(CDSTimeseriesLocationFile, "Taiwan", "country", f, "")
	cnt, rawRecordCount, err = parser.ParseHistory(noEarlier)
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

func CDSDailyUpdate(client *MongoClient, cdsFile string) error {
	f, err := os.Open(cdsFile)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	parserUS := NewCDSParser(CDSDaily, "United State", "county", f, "")
	cnt, err := parserUS.ParseDaily()
	if err != nil {
		fmt.Println("parse US daily error:", err)
	}
	fmt.Println("parse us daily cnt:", cnt)
	err = createCDSData(client, parserUS.Result, CollectionConfirmUS)
	if err != nil {
		fmt.Println("create US CDSData error:", err)
		return err
	}
	f.Seek(0, 0)
	parserTW := NewCDSParser(CDSDaily, "Taiwan", "country", f, "")
	cnt, err = parserTW.ParseDaily()
	f.Close()
	if err != nil {
		fmt.Println("parse Taiwan daily error:", err)
	}
	fmt.Println("parse Taiwan daily cnt:", cnt)
	err = createCDSData(client, parserTW.Result, CollectionConfirmTaiwan)
	if err != nil {
		fmt.Println("create Taiwan CDSData error:", err)
		return err
	}
	return nil
}

func CDSDailyOnline(client *MongoClient, url string) error {
	parserUS := NewCDSParser(CDSDaily, "United State", "county", nil, url)
	cnt, err := parserUS.ParseDailyOnline()
	if err != nil {
		fmt.Println("parse US daily error:", err)
	}
	fmt.Println("parse us daily cnt:", cnt)
	err = createCDSData(client, parserUS.Result, CollectionConfirmUS)
	if err != nil {
		fmt.Println("create US CDSData error:", err)
		return err
	}

	parserTW := NewCDSParser(CDSDaily, "Taiwan", "country", nil, url)
	cnt, err = parserTW.ParseDailyOnline()
	if err != nil {
		fmt.Println("parse US daily error:", err)
	}
	fmt.Println("parse us daily cnt:", cnt)
	err = createCDSData(client, parserTW.Result, CollectionConfirmUS)
	if err != nil {
		fmt.Println("create Taiwan CDSData error:", err)
		return err
	}

	return nil
}
