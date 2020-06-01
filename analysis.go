package main

import (
	"fmt"
)

const defaultWindowSize = 14

type CDSDataPoint struct {
	Name       string
	ReportTime int64   // X - value
	ReportDate string  // X - label
	Score      float64 // Y-value
	Country    string
	State      string
	County     string
}

func ExponientialScoreOfAllTime(c *MongoClient, loc PoliticalGeo) error {
	formula := Exponiential{}

	contData, err := ContinuousDataCDSConfirm(c, loc, defaultWindowSize, 0)
	if err != nil {
		fmt.Println("Error:", err)
	}
	fmt.Println("ContData", contData)
	formula.Score(loc, contData)
	for _, day := range formula.OutputDataPoint {
		fmt.Println(day)
	}
	return nil
}
