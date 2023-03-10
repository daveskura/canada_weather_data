"""
  Dave Skura, 2023

	url = 'https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=10700&Year=2007&Month=11&Day=1&time=&timeframe=2&submit=Download+Data'

"""

import requests
import sys
from postgresdave_package.postgresdave import postgres_db
headers = {
    "content-type": "text/html"
}

mydb = postgres_db()
mydb.connect()
print (" Connecting " + mydb.dbstr()) # 
print(mydb.dbversion())

url_base = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html" + "?"
url_base += "format=csv" + "&"

missing_sql = """
SELECT distinct allthese.station_id,allthese.province,allthese.year -- 49,980 / 287,839
FROM (
    SELECT ws.station_id,ws.province,year
    FROM canweather.weather_station ws
        INNER JOIN canweather.station_years sy ON (ws.station_id::int = sy.station_id::int)
    ) allthese    
LEFT JOIN canweather.station_data sd on (allthese.station_id   = sd.stationid and
                                        allthese.province      = sd.province and
                                        allthese.year::int          = sd."Year"::int )
WHERE sd.stationid is null
limit 10000
"""
stations = mydb.query(missing_sql)
for data in stations:
	stationID = str(data[0])
	province = str(data[1])
	year = str(data[2])
	month = '12'

	url_querystring = "stationID=" + stationID + "&"
	url_querystring += "Year=" + year + "&"
	url_querystring += "Month=" + month + "&"
	url_querystring += "Day=1" + "&"
	url_querystring += "time=" + "&"
	url_querystring += "timeframe=2" + "&"
	url_querystring += "submit=Download+Data"

	url = url_base + url_querystring
	response = requests.post(url, headers=headers)
	filename = 'data/' + stationID + '_' + province + '_' + year + '_' + month + '.csv'
	with open(filename, "w+", encoding="utf-8") as f:
		f.write(response.text)
		f.close()