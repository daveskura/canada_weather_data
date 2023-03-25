"""
  Dave Skura
  
  File Description:

		sqlitedave_package.sqlitedave.sqlite_db('local_sqlite_db').load_csv_to_table('Calendar.tsv','Calendar',True,'\t')
"""
from sqlitedave_package.sqlitedave import sqlite_db

db = sqlite_db()
print(db.dbstr())

print ("loading tables Station,Calendar,Postal_Code_Segments ") # 

db.load_csv_to_table('Calendar.tsv','Calendar',True,'\t')
db.load_csv_to_table('Station.tsv','Station',True,'\t')
db.load_csv_to_table('Postal_Code_Segments.tsv','Postal_Code_Segments',True,'\t')


db.close()



