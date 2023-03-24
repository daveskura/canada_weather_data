"""
  Dave Skura
  
  File Description:
"""
from sqlitedave_package.sqlitedave import sqlite_db

DB_NAME = 'local_sqlite_db'
db = sqlite_db(DB_NAME)
print(db.dbstr())

sql = """

pragma table_info('Calendar'); 

"""
print(db.export_query_to_str(sql))
db.close()