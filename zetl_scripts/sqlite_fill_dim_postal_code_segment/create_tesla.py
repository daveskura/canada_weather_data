"""
  Dave Skura
  
  File Description:
"""
from sqlitedave_package.sqlitedave import sqlite_db

DB_NAME = 'local_sqlite_db'
db = sqlite_db(DB_NAME)
print(db.dbstr())

sql = """
CREATE TABLE tesla (
	date text,
	close real,
	volume integer,
	open real,
	high real,
	low real
);
"""
db.execute(sql)

db.close()