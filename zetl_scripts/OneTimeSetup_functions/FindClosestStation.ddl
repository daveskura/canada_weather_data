/*
  -- Dave Skura, 2022
*/

CREATE OR REPLACE FUNCTION FindClosestStation(
	mylatitude numeric(13,8),
	mylongitude numeric(13,8),
	mydate date
	)
    RETURNS integer
    LANGUAGE 'sql'
    COST 100
    VOLATILE PARALLEL SAFE 
AS $BODY$

SELECT stationid::integer -- ,station_name, province, latitude,longitude,distance
FROM (
    SELECT stationid, latitude, longitude, abs(mylatitude-latitude)+abs(mylongitude-longitude) as distance
        ,stationname, province
    FROM canweather.Station SC
	WHERE mydate between start_dt and end_dt
    ORDER BY 4
    LIMIT 1
) L;

$BODY$;
