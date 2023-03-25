"""
  Dave Skura
  
  File Description:
"""

from sqlitedave_package.sqlitedave import sqlite_db

def main():
	obj = beststation()
	obj.output_postalcode_distance('A0A0A0')
	obj.output_postalcode_distance('A0A1A0')

class beststation:
	def __init__(self):
		self.output_table = 'postalcode_distances'
		self.db = sqlite_db()
		print(self.db.dbstr())
		
	def output_postalcode_distance(self,postalcode):
		if not self.chkoutputtable(self.output_table,postalcode):
			print('postalcode ' + postalcode + ' already done')
			print(self.db.export_query_to_str("SELECT * FROM postalcode_distances WHERE postalcode = '" + postalcode + "'"))
			return False
		else:
			isql_hdr = 'INSERT INTO ' + self.output_table + ' (postalcode,stationid,start_dt,end_dt) VALUES '

			data = self.db.query(self.get_distances_sql(postalcode))
			prvrow_stationid = ''
			prvrow_caldt = ''
			thisrow_stationid = ''
			thisrow_caldt = ''
			for row in data:
				prvrow_stationid = thisrow_stationid
				prvrow_caldt = thisrow_caldt
				thisrow_stationid = row[1]
				thisrow_caldt = row[2]

				if prvrow_stationid == '':
					start_dt = thisrow_caldt
				elif thisrow_stationid != prvrow_stationid:
					isql = "('" + postalcode + "'," + str(prvrow_stationid) + ",'" + start_dt + "','" + prvrow_caldt + "')"
					self.db.execute(isql_hdr + isql)
					start_dt = thisrow_caldt

			isql = "('" + postalcode + "'," + str(thisrow_stationid) + ",'" + start_dt + "','" + thisrow_caldt + "')"
			self.db.execute(isql_hdr + isql)

			return True

	def chkoutputtable(self,tblname = 'postalcode_distances',postalcode='A0A0A0'):
		if not self.db.does_table_exist(tblname):
			sql = """
			CREATE TABLE """ + tblname + """ (
				postalcode text,
				stationid integer,
				start_dt text,
				end_dt text
			);
			"""
			self.db.execute(sql)
			return True
		else:
			sql = "SELECT COUNT(*) FROM " + tblname + " WHERE postalcode = '" + postalcode + "'"
			if self.db.queryone(sql) == 0:
				return True
			else:
				return False

	def get_distances_sql(self,postalcode):
		sql = """
		WITH cte_distances as (
		SELECT A.postalcode,B.stationid,A.caldt,C.distance,pc_latitude,pc_longitude,stn_latitude, stn_longitude
		FROM (  SELECT province, segment,postalcode,caldt,latitude as pc_latitude,longitude as pc_longitude 
						FROM postal_code_segments,
										Calendar
						WHERE postalcode = '""" + postalcode + """'
				 ) A INNER JOIN (
						SELECT province,caldt, stationid,start_dt,end_dt,latitude as stn_latitude, longitude as stn_longitude 
						FROM Station
										INNER JOIN Calendar ON (caldt between start_dt and end_dt)
				 ) B USING (province,caldt) INNER JOIN 
										(SELECT province,segment,postalcode,stationid,distance 
										 FROM (
														SELECT province,segment,PC.postalcode,stationid,
																		abs(PC.latitude-SC.latitude)+abs(PC.longitude-SC.longitude) as distance
														FROM postal_code_segments PC
																		INNER JOIN Station SC USING (province)
														WHERE postalcode = '""" + postalcode + """' 
														) L
										 WHERE postalcode = '""" + postalcode + """'
										) C ON (A.province = C.province and A.segment = C.segment and A.postalcode = C.postalcode and B.stationid = C.stationid)
		)
		SELECT cte_distances.*
		FROM (
				SELECT postalcode,caldt,min(distance) as shortest_distance
				FROM cte_distances
				GROUP BY postalcode,caldt
				) A INNER JOIN cte_distances ON (A.postalcode = cte_distances.postalcode AND A.caldt = cte_distances.caldt and A.shortest_distance = cte_distances.distance)
		ORDER BY cte_distances.caldt
		"""
		return sql

if __name__ == '__main__':
	main()




