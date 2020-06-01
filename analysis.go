package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path"
	"time"
)

const (
	defaultWindowSize = 14
	scoreFile         = ""
)

type CDSDataPoint struct {
	Name       string
	ReportTime int64   // X - value
	ReportDate string  // X - label
	Score      float64 // Y-value
	Country    string
	State      string
	County     string
}

func todayStartAt() int64 {
	curTime := time.Now().UTC()
	start := time.Date(curTime.Year(), curTime.Month(), curTime.Day(), 0, 0, 0, 0, time.UTC)
	return start.Unix()
}
func ExponientialScoreOfAllTime(c *MongoClient, loc PoliticalGeo) error {
	formula := Exponiential{}
	timeBefore := todayStartAt()
	fmt.Println("Today start At:", timeBefore)
	moreData := true
	for moreData {
		contData, err := ContinuousDataCDSConfirm(c, loc, defaultWindowSize, timeBefore)
		if err != nil {
			fmt.Println("Error:", err)
			timeBefore = timeBefore - 86400 - 1 // -1 is because timeBefore is an include function
		}
		if 0 == len(contData) {
			moreData = false
			continue
		}
		formula.Score(loc, contData)
		timeBefore = contData[len(contData)-1].ReportTime - 1
	}
	err := SaveToCVS(formula.OutputDataPoint)
	if err != nil {
		fmt.Println("Write CVS Error:", err)
	}
	return nil
}

func SaveToCVS(data []CDSDataPoint) error {
	records := [][]string{{"name", "date", "timestamp", "score", "country", "state", "county", "level"}}

	for _, record := range data {
		cvsRecord := []string{}
		cvsRecord = append(cvsRecord, record.Name)
		cvsRecord = append(cvsRecord, record.ReportDate)
		cvsRecord = append(cvsRecord, fmt.Sprintf("%d", record.ReportTime))
		cvsRecord = append(cvsRecord, fmt.Sprintf("%f", record.Score))
		cvsRecord = append(cvsRecord, record.Country)
		cvsRecord = append(cvsRecord, record.State)
		cvsRecord = append(cvsRecord, record.County)
		fmt.Println(cvsRecord)
		records = append(records, cvsRecord)
	}
	working, err := os.Getwd()
	if err != nil {
		return err
	}
	fmt.Println("length of reocrds:", len(records))
	if len(records) > 1 {
		filename := records[1][0] + records[1][1] + ".cvs"
		path := path.Join(working, DataDir, filename)
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		w := csv.NewWriter(f)
		w.WriteAll(records)
		if err := w.Error(); err != nil {
			return err
		}
		fmt.Println("write ", len(records)-1, " records to CVS")
	}

	return nil
}
