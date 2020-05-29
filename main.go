package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"github.com/bitmark-inc/autonomy-api/schema"
)

var USAData []schema.CDSData

const (
	DataDir                  = "data"
	CollectionConfirmUS      = "ConfirmUS"
	CollectionConfirmTaiwan  = "ConfirmTaiwan"
	CollectionConfirmIceland = "ConfirmIceland"
	DuplicateKeyCode         = 11000
	coronaDataScraperURL     = "https://coronadatascraper.com/data.json"
	keepDaysInHistory        = 30
)

var job string
var country string

func init() {
	flag.StringVar(&job, "job", "history", "select from history/daily/online")
	flag.StringVar(&country, "country", "country", "ie. United States / Taiwan / Iceland")
}

func main() {
	//go PrintUsage()
	flag.Parse()

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
		keepDays := time.Now().UTC().Unix() - 60*60*24*keepDaysInHistory
		CDSHistoryToDB(client, file, country, keepDays)
	case "daily":
		file, _ := getDataFilePath(CDSDaily)
		log.Println("filepath=", file)
		err := CDSDailyUpdate(client, file, country)
		if err != nil {
			fmt.Println("parse daily err", err)
		}
	case "online":
		err := CDSDailyOnline(client, coronaDataScraperURL, country)
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

func CDSHistoryToDB(client *MongoClient, cdsFile string, country string, noEarlier int64) {
	fmt.Println("CDSDailyUpdate:", " parse file:", cdsFile, " country:", country, " noEarlier:", noEarlier)
	f, err := os.Open(cdsFile)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer f.Close()

	switch country {
	case "United States":
		parser := NewCDSParser(CDSTimeseriesLocationFile, "United State", "county", f, "")
		cnt, rawRecordCount, err := parser.ParseHistory(noEarlier)
		if err != nil {
			log.Println("US Data Parse Error", err)
			return
		}
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
	case "Taiwan":
		err = setIndex(client, CollectionConfirmTaiwan)
		if err != nil {
			fmt.Println("set ConfirmTW index error:", err)
			return
		}
		parserTW := NewCDSParser(CDSTimeseriesLocationFile, "Taiwan", "country", f, "")
		cnt, rawRecordCount, err := parserTW.ParseHistory(noEarlier)
		if err != nil {
			log.Println("Taiwan  Data Parse Error", err)
			return
		}
		log.Println("Taiwan data get:", cnt, " rawRecordCount in file:", rawRecordCount)
		err = createCDSData(client, parserTW.Result, CollectionConfirmTaiwan)
		if err != nil {
			fmt.Println("create Taiwan CDSData error:", err)
			return
		}
	case "Iceland":
		err = setIndex(client, CollectionConfirmIceland)
		if err != nil {
			fmt.Println("set confirm Iceland index error:", err)
			return
		}
		parserIceland := NewCDSParser(CDSTimeseriesLocationFile, "Iceland", "country", f, "")
		cnt, rawRecordCount, err := parserIceland.ParseHistory(noEarlier)
		if err != nil {
			log.Println("Iceland  Data Parse Error", err)
			return
		}
		log.Println("Iceland data get:", cnt, " rawRecordCount in file:", rawRecordCount)
		err = createCDSData(client, parserIceland.Result, CollectionConfirmIceland)
		if err != nil {
			fmt.Println("create Iceland CDSData error:", err)
			return
		}
	default:
		fmt.Println("No Data Set for ", country)
	}
}

func CDSDailyUpdate(client *MongoClient, cdsFile string, country string) error {
	fmt.Println("CDSDailyUpdate:", " parse file:", cdsFile, " country:", country)
	f, err := os.Open(cdsFile)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	defer f.Close()
	switch country {
	case "United States":
		parserUS := NewCDSParser(CDSDaily, "United State", "county", f, "")
		cnt, err := parserUS.ParseDaily()
		if err != nil {
			fmt.Println("parse US daily error:", err)
			return err
		}
		fmt.Println("parse us daily cnt:", cnt)
		err = ReplaceCDS(client, parserUS.Result, CollectionConfirmUS)
		if err != nil {
			fmt.Println("create US CDSData error:", err)
			return err
		}
	case "Taiwan":
		parserTW := NewCDSParser(CDSDaily, "Taiwan", "country", f, "")
		cnt, err := parserTW.ParseDaily()
		f.Close()
		if err != nil {
			fmt.Println("parse Taiwan daily error:", err)
			return err
		}
		fmt.Println("parse Taiwan daily cnt:", cnt)
		err = ReplaceCDS(client, parserTW.Result, CollectionConfirmTaiwan)
		if err != nil {
			fmt.Println("create Taiwan CDSData error:", err)
			return err
		}
	case "Iceland":
		parserIceland := NewCDSParser(CDSDaily, "Iceland", "country", f, "")
		cnt, err := parserIceland.ParseDaily()
		f.Close()
		if err != nil {
			fmt.Println("parse Iceland daily error:", err)
			return err
		}
		fmt.Println("parse Taiwan daily cnt:", cnt)
		err = ReplaceCDS(client, parserIceland.Result, CollectionConfirmIceland)
		if err != nil {
			fmt.Println("create Iceland CDSData error:", err)
			return err
		}
	default:
		fmt.Println("No Data Set for ", country)
		return errors.New("country has no data-set")
	}
	return nil
}

func CDSDailyOnline(client *MongoClient, url string, country string) error {
	fmt.Println("CDSDailyOnline:", " url:", url, " country:", country)
	switch country {
	case "United States":
		parserUS := NewCDSParser(CDSDaily, "United States", "county", nil, url)
		cnt, err := parserUS.ParseDailyOnline()
		if err != nil {
			fmt.Println("parse US daily error:", err)
			return err
		}
		fmt.Println("parse us daily cnt:", cnt)
		err = ReplaceCDS(client, parserUS.Result, CollectionConfirmUS)
		if err != nil {
			fmt.Println("create US CDSData error:", err)
			return err
		}
	case "Taiwan":
		parserTW := NewCDSParser(CDSDaily, "Taiwan", "country", nil, url)
		cnt, err := parserTW.ParseDailyOnline()
		if err != nil {
			fmt.Println("parse TW daily error:", err)
			return err
		}
		fmt.Println("parse tw daily cnt:", cnt)
		err = ReplaceCDS(client, parserTW.Result, CollectionConfirmTaiwan)
		if err != nil {
			fmt.Println("create Taiwan CDSData error:", err)
			return err
		}
	case "Iceland":
		parserIceland := NewCDSParser(CDSDaily, "Iceland", "country", nil, url)
		cnt, err := parserIceland.ParseDailyOnline()
		if err != nil {
			fmt.Println("parse Iceland daily error:", err)
			return err
		}
		fmt.Println("parse Iceland daily cnt:", cnt)
		err = ReplaceCDS(client, parserIceland.Result, CollectionConfirmIceland)
		if err != nil {
			fmt.Println("create Iceland CDSData error:", err)
			return err
		}
	default:
		fmt.Println("No Data Set for ", country)
		return errors.New("country has no data-set")
	}

	return nil
}
