"""
  Dave Skura
  
  File Description:
"""
from sqlitedave_package.sqlitedave import sqlite_db

db = sqlite_db()
print(db.dbstr())

sql = """

SELECT *
FROM (
	SELECT count(*) as stations
	FROM Station 
	) A,
  (
	SELECT count(*) as days
	FROM Calendar
	) B,
  (
	SELECT count(DISTINCT postalcode) as total_postalcodes
	FROM Postal_Code_Segments
	) C,
  (
	SELECT count(DISTINCT postalcode) as todo_postalcodes
	FROM whatarewedoing
	) D,
	(
	SELECT count(DISTINCT postalcode) as postalcodes_done
	FROM whatarewedoing
	WHERE who <> ''
	) E

"""
data = db.export_query_to_str(sql)
print(data)

db.close()