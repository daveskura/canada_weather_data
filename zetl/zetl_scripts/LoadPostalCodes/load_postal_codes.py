"""
  Dave Skura, 2021
  
  File Description:
"""
import psycopg2 
import os
import sys
sys.path.append("..\..") # Adds higher directory to python modules path.
from postgresdatabase import db

tblname = 'weather.postal_codes'
mydb = db()
mydb.connect()
print (" Connecting " + mydb.connection_str) # 
print(mydb.get_dbversion())

csvfile = 'CanadianPostalCodes.csv'
mydb.load_csv_to_table(csvfile,tblname,False,',')
print(csvfile)
