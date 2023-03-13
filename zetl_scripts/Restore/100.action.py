"""
  Dave Skura, 2021
  
  File Description:
"""
import os
import sys
from schemawizard_package.schemawizard import schemawiz

Here = 'c:\\git\\canada_weather_data\\zetl_scripts\Restore'
dir_list = os.listdir(Here)
for fil in dir_list:
	if fil.lower().endswith('.tsv') or fil.lower().endswith('.csv') and fil.lower() != 'z_etl.csv':
		filparts = fil.split('.')
		try:
			ext = filparts[2]
			tablename = filparts[1]
			schema = filparts[0]
		except:
			ext = filparts[1]
			tablename = filparts[0]
			schema = 'public'
		
		csv_filename = Here + '\\' + fil
		WithTruncate = True
		tbl = schema + '.' + tablename
		print('import table: ' + tbl + ' from ' + csv_filename) # 
	
		obj = schemawiz()
		if obj.dbthings.postgres_db.does_table_exist(tbl):
			obj.justload_postgres_from_csv(csv_filename,tbl,True)
		else:
			ddlfilename = 'z.' + tbl + '.ddl'
			obj.createload_postgres_from_csv(csv_filename,tbl,ddlfilename)
