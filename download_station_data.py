"""
  Dave Skura, 2023

	url = 'https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=10700&Year=2007&Month=11&Day=1&time=&timeframe=2&submit=Download+Data'

"""


import requests
headers = {
    "content-type": "text/html"
}

url_base = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html" + "?"
url_querystring = "format=csv" + "&"
url_querystring += "stationID=10700" + "&"
url_querystring += "Year=2007" + "&"
url_querystring += "Month=11" + "&"
url_querystring += "Day=1" + "&"
url_querystring += "time=" + "&"
url_querystring += "timeframe=2" + "&"
url_querystring += "submit=Download+Data"

url = url_base + url_querystring
print(url)
response = requests.post(url, headers=headers)

with open('station.csv', "w", encoding="utf-8") as f:
	f.write(response.text)
	f.close()
