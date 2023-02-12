"""
  Dave Skura, 2021
  
  File Description:
"""

import psycopg2 
import os
import sys
sys.path.append("..\..") # Adds higher directory to python modules path.
from postgresdatabase import db

tblname = 'weather.station_data'
hdrs='loadfile,stationid,province,"Longitude (x)","Latitude (y)","Station Name","Climate ID","Date/Time","Year","Month","Day","Data Quality","Max Temp (C)","Max Temp Flag","Min Temp (C)","Min Temp Flag","Mean Temp (C)","Mean Temp Flag","Heat Deg Days (C)","Heat Deg Days Flag","Cool Deg Days (C)","Cool Deg Days Flag","Total Rain (mm)","Total Rain Flag","Total Snow (cm)","Total Snow Flag","Total Precip (mm)","Total Precip Flag","Snow on Grnd (cm)","Snow on Grnd Flag","Dir of Max Gust (10s deg)","Dir of Max Gust Flag","Spd of Max Gust (km/h)","Spd of Max Gust Flag"'
mydb = db()
mydb.connect()
print (" Connecting " + mydb.connection_str) # 
print(mydb.get_dbversion())
datapath = '../../../data'
tgtpath = '../../../loaded'
for f in os.listdir(datapath):
	csvfile = datapath + '/' + f
	tgtfile = tgtpath + '/' + f
	
	withextrafields = {'loadfile':f, 'stationid':f.split('_')[0], 'province':f.split('_')[1] }
	mydb.load_csv_to_table(csvfile,tblname,False,',',hdrs,withextrafields)
	os.replace(csvfile,tgtfile)
	print(csvfile)
	
