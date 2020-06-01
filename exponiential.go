package main

import (
	"math"
)

type Exponiential struct {
	OutputDataPoint []CDSDataPoint
}

func (e *Exponiential) Score(loc PoliticalGeo, data []CDSScoreDataSet) {
	score, name, reportTime, reportDate := e.calculateScore(data)
	if len(data) > 0 {
		dataPoint := CDSDataPoint{Name: name, ReportTime: reportTime, ReportDate: reportDate, Score: score, Country: loc.Country, State: loc.State, County: loc.County}
		e.OutputDataPoint = append(e.OutputDataPoint, dataPoint)
	}
	return

}

func (e *Exponiential) calculateScore(dataset []CDSScoreDataSet) (float64, string, int64, string) {
	score := float64(0)
	sizeOfConfirmData := len(dataset)
	reportTime := int64(0)
	reportDate := ""
	if 0 == len(dataset) {
		return 0, "", reportTime, reportDate
	} else if len(dataset) < defaultWindowSize {
		reportTime = dataset[sizeOfConfirmData-1].ReportTime
		reportDate = dataset[sizeOfConfirmData-1].ReportDate
		zeroDay := []CDSScoreDataSet{CDSScoreDataSet{Name: dataset[0].Name, Cases: 0}}
		for idx := 0; idx < defaultWindowSize-sizeOfConfirmData; idx++ {
			dataset = append(zeroDay, dataset...)
		}
	} else {
		reportTime = dataset[sizeOfConfirmData-1].ReportTime
		reportDate = dataset[sizeOfConfirmData-1].ReportDate
	}
	//	fmt.Println("reprotDate", reportDate, " reportTime:", reportTime)

	numerator := float64(0)
	denominator := float64(0)
	for idx, val := range dataset {
		power := (float64(idx) + 1) / 2
		numerator = numerator + math.Exp(power)*val.Cases
		denominator = denominator + math.Exp(power)*(val.Cases+1)
	}

	if denominator > 0 {
		score = 1 - numerator/denominator
	}
	return score * 100, dataset[sizeOfConfirmData-1].Name, reportTime, reportDate
}
