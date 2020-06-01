package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/spf13/viper"
)

const (
	default_mongo_conn        = "mongodb://localhost:27017"
	default_mongo_autonomy_db = "autonomy"
)
const (
	CdsUSA     = "United States"
	CdsTaiwan  = "Taiwan"
	CdsIceland = "Iceland"
)

type CDSCountryType string

var CDSCountyCollectionMatrix = map[CDSCountryType]string{
	CDSCountryType(CdsUSA):     "ConfirmUS",
	CDSCountryType(CdsTaiwan):  "ConfirmTaiwan",
	CDSCountryType(CdsIceland): "ConfirmIceland",
}

var defaulMognoTimeout = 5 * time.Second

var (
	ErrNoConfirmDataset       = fmt.Errorf("no data-set")
	ErrInvalidConfirmDataset  = fmt.Errorf("invalid confirm data-set")
	ErrPoliticalTypeGeoInfo   = fmt.Errorf("no political type geo info")
	ErrConfirmDataFetch       = fmt.Errorf("fetch cds confirm data fail")
	ErrConfirmDecode          = fmt.Errorf("decode confirm data fail")
	ErrConfirmDuplicateRecord = fmt.Errorf("confirm data duplicate")
)

type CDSScoreDataSet struct {
	Name       string  `json:"name" bson:"name"`
	ReportTime int64   `json:"report_ts" bson:"report_ts"`
	ReportDate string  `json:"report_date" bson:"report_date"`
	Cases      float64 `json:"cases" bson:"cases"`
}

type PoliticalGeo struct {
	Country string
	State   string
	County  string
}

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
	fmt.Println("createCDSData try to insert ", len(data), " record")
	opts := options.InsertMany().SetOrdered(false)
	res, err := c.UsedDB.Collection(collection).InsertMany(context.Background(), data, opts)
	if err != nil {
		if errs, hasErr := err.(mongo.BulkWriteException); hasErr {
			if 1 == len(errs.WriteErrors) && DuplicateKeyCode == errs.WriteErrors[0].Code {
				fmt.Println(err)
				return nil
			}
		}
	}
	if res != nil {
		fmt.Println("number record inserted:", len(res.InsertedIDs))
	} else {
		fmt.Println("no record inserted in db")
	}

	return nil
}

func ReplaceCDS(c *MongoClient, result []CDSData, collection string) error {
	for _, v := range result {
		filter := bson.M{"name": v.Name, "report_ts": v.ReportTime}
		replacement := bson.M{
			"name":        v.Name,
			"city":        v.City,
			"county":      v.County,
			"state":       v.State,
			"country":     v.Country,
			"level":       v.Level,
			"cases":       v.Cases,
			"deaths":      v.Deaths,
			"recovered":   v.Recovered,
			"report_ts":   v.ReportTime,
			"update_ts":   v.UpdateTime,
			"report_date": v.ReportTimeDate,
			"countryId":   v.CountryID,
			"stateId":     v.StateID,
			"countyId":    v.CountyID,
			"location":    v.Location,
			"tz":          v.Timezone,
		}
		opts := options.Replace().SetUpsert(true)
		_, err := c.UsedDB.Collection(collection).ReplaceOne(context.Background(), filter, replacement, opts)
		if err != nil {
			if errs, hasErr := err.(mongo.BulkWriteException); hasErr {
				if 1 == len(errs.WriteErrors) && DuplicateKeyCode == errs.WriteErrors[0].Code {
					fmt.Println("cds update with error: %s", err)
				}
			}
		}
	}
	return nil
}

//
func ContinuousDataCDSConfirm(c *MongoClient, loc PoliticalGeo, windowSize int64, timeBefore int64) ([]CDSScoreDataSet, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaulMognoTimeout)
	defer cancel()

	var col *mongo.Collection
	var filter bson.M
	var opts *options.FindOptions
	switch loc.Country { //  Currently this function support only USA data
	case CdsTaiwan:
		col = c.UsedDB.Collection(CDSCountyCollectionMatrix[CDSCountryType(CdsTaiwan)])
		opts = options.Find().SetSort(bson.M{"report_ts": -1}).SetLimit(windowSize + 1)
		filter = bson.M{}
		if timeBefore > 0 {
			filter = bson.M{"report_ts": bson.D{{"$lte", timeBefore}}}
		}
	case CdsIceland:
		col = c.UsedDB.Collection(CDSCountyCollectionMatrix[CDSCountryType(CdsIceland)])
		opts = options.Find().SetSort(bson.M{"report_ts": -1}).SetLimit(windowSize + 1)
		filter = bson.M{}
		if timeBefore > 0 {
			filter = bson.M{"report_ts": bson.D{{"$lte", timeBefore}}}
		}
	case CdsUSA:
		col = c.UsedDB.Collection(CDSCountyCollectionMatrix[CDSCountryType(CdsUSA)])
		opts = options.Find().SetSort(bson.M{"report_ts": -1}).SetLimit(windowSize + 1)
		if "" == loc.State || "" == loc.County {
			return nil, ErrNoConfirmDataset
		}
		filter = bson.M{"county": loc.County, "state": loc.State}
		if timeBefore > 0 {
			filter = bson.M{"county": loc.County, "state": loc.State, "report_ts": bson.D{{"$lte", timeBefore}}}
		}

	default:
		return nil, ErrNoConfirmDataset
	}

	var results []CDSScoreDataSet
	cur, err := col.Find(context.Background(), filter, opts)
	if nil != err {
		return nil, ErrConfirmDataFetch
	}

	now := CDSScoreDataSet{}

	for cur.Next(ctx) {
		var result CDSScoreDataSet
		if errDecode := cur.Decode(&result); errDecode != nil {
			return nil, errDecode
		}
		if len(now.Name) > 0 { // now data is valid
			head := make([]CDSScoreDataSet, 1)
			head[0] = CDSScoreDataSet{Name: now.Name, Cases: now.Cases - result.Cases, ReportTime: now.ReportTime, ReportDate: now.ReportDate}
			results = append(head, results...)
		}
		now = result
	}
	if len(results) == 0 && now.Name != "" { // only one record
		results = append(results, now)
	}

	cur.Close(ctx)
	return results, nil
}
