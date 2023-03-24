"""
  Dave Skura
  
  File Description:
"""
from zetl.run import zetl
from postgresdave_package.postgresdave import postgres_db 


print (" Starting ") # 
nxtseg_sql = """
SELECT min(all_segments) as nxt_segment
FROM (
    SELECT S.segment as all_segments,filled_segments.segment,counts
    FROM (
        SELECT distinct segment 
        from canweather.postal_code_segments
        ) S LEFT JOIN  (
            SELECT segment,count(*) as counts -- 23874 
            FROm canweather.dim_postal_code dpc
                INNER JOIN canweather.postal_code_segments pcs ON (dpc.postalcode = pcs.postalcode)
            GROUP BY segment
        ) filled_segments ON (S.segment = filled_segments.segment)
    ORDER BY S.segment
    ) L
WHERE segment is null
"""
db = postgres_db()
db.connect()
nxtseg = db.queryone(nxtseg_sql)
while nxtseg != None:
	print('Working on segment: ' + str(nxtseg))
	zetl().proper_run('repeatcall_dim_postal_code_segment',str(nxtseg))
	nxtseg = db.queryone(nxtseg_sql)
	db.execute('VACUUM VERBOSE')
	

print('nothing left to do.  All segments loaded.')