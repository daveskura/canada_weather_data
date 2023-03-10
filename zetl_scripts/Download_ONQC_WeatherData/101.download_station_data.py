"""
  Dave Skura, 2023

	url = 'https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=10700&Year=2007&Month=11&Day=1&time=&timeframe=2&submit=Download+Data'

"""
PROVINCE = "('ON','QC')"

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


stations = mydb.query("SELECT station_id,province FROM canweather.weather_station WHERE province IN " + PROVINCE + " ORDER BY station_id")
for data in stations:
	stationID = str(data[0])
	province = str(data[1])
	years = mydb.query('SELECT year FROM canweather.station_years WHERE station_id::int = ' + stationID + ' ORDER BY year')
	months = mydb.query('SELECT month FROM canweather.station_months WHERE station_id::int = ' + stationID + ' ORDER BY month')
	for yr_data in years:
		year = str(yr_data[0])

		#for mth_data in months:
		month = '12' # str(mth_data[0])

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