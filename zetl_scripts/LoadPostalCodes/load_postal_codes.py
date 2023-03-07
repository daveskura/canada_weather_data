"""
  Dave Skura, 2021
  
  File Description:
"""
import os
import sys
from postgresdave_package.postgresdave import db 
mydb = db()

mydb.connect()
print(mydb.dbversion())

tblname = 'weather.postal_codes'
csvfile = 'CanadianPostalCodes.csv'
mydb.load_csv_to_table(csvfile,tblname,True,',')
print(csvfile)

