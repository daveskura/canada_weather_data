/*
  -- Dave Skura, 2022
*/

CREATE OR REPLACE FUNCTION FindClosestStation(
	mysegment integer,
	mypostal_code varchar(15),
	mydate date
	)
    RETURNS integer
    LANGUAGE 'sql'
    COST 100
    VOLATILE PARALLEL SAFE 
AS $BODY$


SELECT D.stationid::integer 
FROM canweather.distances D
    INNER JOIN canweather.Station S ON (D.stationid = S.stationid)
WHERE mydate between S.start_dt and end_dt and
    D.postalcode=mypostal_code and
	D.segment = mysegment
ORDER BY D.distance 
LIMIT 1;


$BODY$;


