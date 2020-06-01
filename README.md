# parseCoronaData
Parse [corona data scraper data] (https://coronadatascraper.com/)(CDS) and save them into MongoDB by Country. 

### Support Data Set
+ "United States"
+ "Taiwan"
+ "Iceland"



## Download Files 
### History Data
+ save location history data into 
    ```{paraseCoronaData Project Folder}/data/timeseries-byLocation.json```
### Daily Data
+ save daily file into 
     ```{paraseCoronaData Project Folder}/data/dataDaily.json```
+ use job "online"
    Get data from http.

## Usage 

```
Usage of ./parseCoronaData:
  -country string
        ie. United States / Taiwan / Iceland (default "country")
  -job string
        select from history/daily/online (default "history")
```
### Examples
+ Download Location based History Data
```
./parseCoronaData  -job historyDownload

```

+ Save All history to
```
./parseCoronaData  -job historyAll -country "United States" 

./parseCoronaData -job historyAll  -country "Taiwan"

./parseCoronaData  -job historyAll -country "Iceland"

```
+ Parse JSON(Location)

```
./parseCoronaData  -job history -country "United States" 

./parseCoronaData -job history  -country "Taiwan"

./parseCoronaData  -job history -country "Iceland"

```

+ Parse Daily online

```
./parseCoronaData  -job dailyOnline -country "United States" 

./parseCoronaData -job dailyOnline  -country "Taiwan"

./parseCoronaData  -job dailyOnline -country "Iceland"

```
+ Save  Analysis Data Point to CVS
```
./parseCoronaData -job analysis  -country "Taiwan"
./parseCoronaData -job analysis  -country "Iceland"

```


