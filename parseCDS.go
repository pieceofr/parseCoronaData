package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

type CovidSource string

const (
	layoutISO = "2006-01-02"
)
const (
	CDSDaily              CovidSource = "daily"
	CDSTimeseriesLocation CovidSource = "timeSeriesLocation"
	CDSTimeseriesByDate   CovidSource = "timeSeriesByDate"
)

type CovidParser interface {
	Parse() (int, error)
}

type CDSParser struct {
	Country     string
	Level       string
	CDSDataType CovidSource
	DataFile    *os.File
	Result      []CDSData
}

type CDSData struct {
	Name           string   `json:"name" bson:"name"`
	City           string   `json:"city" bson:"city"`
	County         string   `json:"county" bson:"county"`
	State          string   `json:"state" bson:"state"`
	Country        string   `json:"country" bson:"country"`
	Level          string   `json:"level" bson:"level"`
	CountryID      string   `json:"countryId" bson:"countryId"`
	StateID        string   `json:"stateId" bson:"stateId"`
	CountyID       string   `json:"countyId" bson:"countyId"`
	Location       GeoJSON  `json:"location" bson:"location"`
	Timezone       []string `json:"tz" bson:"tz"`
	Cases          float64  `json:"cases" bson:"cases"`
	Deaths         float64  `json:"deaths" bson:"deaths"`
	Recovered      float64  `json:"recovered" bson:"recovered"`
	ReportTime     int64    `json:"report_ts" bson:"report_ts"`
	UpdateTime     int64    `json:"update" , bson:"update"`
	ReportTimeDate string   `json:"report_date" bson:"report_date"`
}
type GeoJSON struct {
	Type        string    `bson:"type"`
	Coordinates []float64 `bson:"coordinates"`
}

func NewCDSParser(source CovidSource, country string, level string, input *os.File) CDSParser {
	return CDSParser{Country: country, Level: level, CDSDataType: source, DataFile: input}
}

func (c *CDSParser) ParseHistory() (int, int, error) {
	dec := json.NewDecoder(c.DataFile)
	count := 0
	rawRecordCount := 0
	sourceData := make(map[string]interface{})
	if err := dec.Decode(&sourceData); err != nil {
		return 0, 0, err
	}
	records := []CDSData{}
	for key, value := range sourceData {
		m := value.(map[string]interface{})
		if strings.Contains(key, c.Country) {
			rawRecordCount++
			dateData := m["dates"].(map[string]interface{})
			for k, v := range dateData {
				record := CDSData{}
				ok := false
				record.Name, ok = m["name"].(string)
				if !ok || len(record.Name) <= 0 {
					break
				}
				record.City, _ = m["city"].(string)
				record.County, _ = m["county"].(string)
				record.State, _ = m["state"].(string)

				record.Country, ok = m["country"].(string)
				if !ok || len(record.Country) <= 0 {
					break
				}
				record.Level, _ = m["level"].(string)

				if record.Level != c.Level {
					break
				}
				record.CountryID, _ = m["countryId"].(string)
				record.StateID, _ = m["stateId"].(string)
				record.CountryID, _ = m["countyId"].(string)
				coorRaw, ok := m["coordinates"].([]interface{})
				if ok && len(coorRaw) > 0 {
					coortemp := []float64{}
					for _, coorV := range coorRaw {
						coortemp = append(coortemp, coorV.(float64))
					}
					record.Location = GeoJSON{Type: "Point", Coordinates: coortemp}
				} else {
					record.Location = GeoJSON{Type: "Point", Coordinates: []float64{}}
				}

				tzRaw, ok := m["tz"].([]interface{})
				if ok && len(tzRaw) > 0 {
					tztemp := []string{}
					for _, tzV := range tzRaw {
						tztemp = append(tztemp, tzV.(string))
					}
					record.Timezone = tztemp
				} else {
					record.Timezone = []string{}
				}

				dateCases, ok := v.(map[string]interface{})
				if !ok {
					fmt.Println("cast date data error")
					break
				}
				record.Cases, ok = dateCases["cases"].(float64)
				if !ok {
					break
				}
				record.Deaths, _ = dateCases["deaths"].(float64)
				record.Recovered, _ = dateCases["Recovered"].(float64)

				convertZone := "UTC"
				if len(record.Timezone) > 0 {
					convertZone = record.Timezone[0]
				}
				ts, err := convertLocalDateToUTC(convertZone, k)
				if err != nil {
					fmt.Println(record.Name, "  convertLocalDateToUTC error:", err)
					break
				}
				record.ReportTime = ts
				record.UpdateTime = time.Now().UTC().Unix()
				record.ReportTimeDate = k
				records = append(records, record)
				count++
			}
		}
	}
	c.Result = records
	return count, rawRecordCount, nil
}

func convertLocalDateToUTC(tz string, date string) (int64, error) {
	location, err := time.LoadLocation(tz)
	if err != nil {
		fmt.Errorf("loadLocation error:%v and use UTC instead", err)
		t, parseErr := time.Parse(layoutISO, date)
		if parseErr != nil {
			return 0, parseErr
		}
		return t.Unix(), nil
	}
	t, parseErr := time.ParseInLocation(layoutISO, date, location)
	if parseErr != nil {
		return 0, parseErr
	}
	return t.UTC().Unix(), nil
}
func (c *CDSParser) ParseDaily() (int, error) {
	dec := json.NewDecoder(c.DataFile)
	count := 0
	updateRecords := []CDSData{}

	sourceData := make([]interface{}, 0)
	if err := dec.Decode(&sourceData); err != nil {
		return 0, err
	}
	for _, value := range sourceData {
		record := CDSData{}
		object := value.(map[string]interface{})
		name, ok := object["name"].(string)
		if ok && len(name) > 0 && strings.Contains(name, c.Country) { // Country
			record.Name = name
		} else {
			continue
		}
		record.City, _ = object["city"].(string)
		record.County, _ = object["county"].(string)
		record.State, _ = object["state"].(string)
		record.CountryID, _ = object["countryId"].(string)
		record.StateID, _ = object["stateId"].(string)
		record.CountyID, _ = object["countyId"].(string)
		coorRaw, ok := object["coordinates"].([]interface{})
		if ok && len(coorRaw) > 0 {
			coortemp := []float64{}
			for _, coorV := range coorRaw {
				coortemp = append(coortemp, coorV.(float64))
			}
			record.Location = GeoJSON{Type: "Point", Coordinates: coortemp}
		} else {
			record.Location = GeoJSON{Type: "Point", Coordinates: []float64{}}
		}

		tzRaw, ok := object["tz"].([]interface{})
		if ok && len(tzRaw) > 0 {
			tztemp := []string{}
			for _, tzV := range tzRaw {
				tztemp = append(tztemp, tzV.(string))
			}
			record.Timezone = tztemp
		} else {
			record.Timezone = []string{}
		}
		record.Cases, ok = object["cases"].(float64)
		if !ok {
			fmt.Println("cast cases error")
			continue
		}
		record.Deaths, _ = object["deaths"].(float64)
		record.Recovered, _ = object["Recovered"].(float64)
		dateString := ""
		year, month, day := time.Now().Date()
		dateString = fmt.Sprintf("%s-%s-%s", year, month, day)
		if len(record.Timezone) > 0 {
			location, err := time.LoadLocation(record.Timezone[0])
			if err == nil {
				year, month, day := time.Now().In(location).Date()
				dateString = fmt.Sprintf("%s-%s-%s", year, month, day)
			}
		}
		record.UpdateTime = time.Now().UTC().Unix()
		record.ReportTime = record.UpdateTime
		record.ReportTimeDate = dateString
		count++
		updateRecords = append(updateRecords, record)
	}
	return count, nil
}
