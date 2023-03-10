"""
  Dave Skura, 2021
  
  File Description:
"""

import sys

from postgresdave_package.postgresdave import postgres_db

mydb = postgres_db()
mydb.connect()
print(" Connecting " + mydb.dbstr()) # 
print(mydb.dbversion())

print (" ") # 
f = open('zetl_scripts/OneTimeLoad_WeatherStations/CanadaWeatherStations.csv','r')
content = f.readlines()
for i in range(0,len(content)):
	line = content[i].replace("'",'').split('\t')
	#print(line)
	isql = 'INSERT INTO canweather.weather_station (station_id,station_name,province,webid) VALUES ('
	isql += "'" + line[0] + "','" + line[3].split('=')[1] + "','" + line[1].split('=')[1] + "','" + line[2].split('=')[1] + "');\n"
	years_str = line[4].split('=')[1].replace("'",'')
	years = years_str[1:-1].split(',')
	
	months_str = line[5].split('=')[1].replace("'",'').strip().replace(' ','')
	months = months_str[1:-1].split(',')

	isql += 'INSERT INTO canweather.station_years(station_id,year) VALUES '
	for j in range(0,len(years)):
		isql += "('" + str(line[0]) + "'," + str(years[j]) + '),'

	isql = isql[:-1] + ';\n'
	
	isql += 'INSERT INTO canweather.station_months(station_id,month) VALUES '
	for k in range(0,len(months)):
		isql += "('" + str(line[0]) + "'," + str(months[k]) + '),'

	isql = isql[:-1] + ';\n'
	mydb.execute(isql)
	#print(isql)
	#break
f.close()
print('done')
