"""
  Dave Skura
  
  File Description:
"""
from sqlitedave_package.sqlitedave import sqlite_db

FROM_SEGMENT = '124'
TO_SEGMENT = '150'
DB_NAME = 'local_sqlite_db'
db = sqlite_db(DB_NAME)
print(db.dbstr())

isql = "DELETE FROM whatarewedoing"

db.execute(isql)

isql = """
	INSERT INTO whatarewedoing 
	SELECT DISTINCT '' as who,pcs.postalcode,'' as isdoingwhat 
	FROM Postal_Code_Segments pcs
		LEFT JOIN Dim_Postal_Code dpc ON (pcs.postalcode = dpc.postalcode)
	WHERE dpc.postalcode is null and segment >= """ + FROM_SEGMENT + ' AND segment <= ' + TO_SEGMENT

db.execute(isql)

db.close()