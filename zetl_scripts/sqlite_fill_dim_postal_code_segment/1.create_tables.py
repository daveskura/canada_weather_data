"""
  Dave Skura
  
  File Description:
"""
from sqlitedave_package.sqlitedave import sqlite_db

DB_NAME = 'local_sqlite_db'
db = sqlite_db(DB_NAME)
print(db.dbstr())
print (" create table Station,Calendar,Postal_Code_Segments ") # 
sql = """
DROP TABLE IF EXISTS  Dim_Postal_Code;
"""
db.execute(sql)

sql = """
CREATE TABLE Dim_Postal_Code (
	postalcode	text,
	closest_stationid	integer,
	eff_from_dt	text,
	eff_to_dt	text
);
"""
db.execute(sql)

sql = """
CREATE INDEX Dim_Postal_Codeidx ON Dim_Postal_Code(postalcode);
"""
db.execute(sql)

sql = """
DROP TABLE IF EXISTS  Station;
"""
db.execute(sql)

sql = """
CREATE TABLE Station (
	stationid	integer,
	stationname	text,
	province	text,
	latitude	real,
	longitude	real,
	start_dt	text,
	end_dt text
);
"""
db.execute(sql)

sql = """
CREATE INDEX Stationidx ON Station(province);
"""

db.execute(sql)

sql = """
DROP TABLE IF EXISTS Calendar;
"""
db.execute(sql)

sql = """
CREATE TABLE Calendar (
	year	integer,
	month	integer,
	day	integer,
	caldt text
);
"""

db.execute(sql)

sql = """
CREATE INDEX Calendaridx on Calendar(caldt);
"""
db.execute(sql)

sql = """
DROP TABLE IF EXISTS Postal_Code_Segments;
"""
db.execute(sql)


sql = """
CREATE TABLE Postal_Code_Segments (
	province	text,
	postalcode	text,
	fsa	text,
	latitude	real,
	longitude	real,
	rnk	integer,
	segment integer
);
"""

db.execute(sql)

sql = """
CREATE INDEX Postal_Code_Segmentsidx ON Postal_Code_Segments(postalcode);
"""

db.execute(sql)

sql = """
CREATE INDEX Postal_Code_Segmentsidx2 ON Postal_Code_Segments(province);
"""

db.execute(sql)

sql = """
DROP TABLE IF EXISTS whatarewedoing;
"""
db.execute(sql)

sql = """
CREATE TABLE whatarewedoing (
	who	text,
	postalcode	text,
	isdoingwhat	text
);
"""

db.execute(sql)

db.close()