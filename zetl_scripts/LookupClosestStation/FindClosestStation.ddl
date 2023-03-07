/*
  -- Dave Skura, 2022
*/

@DELIMITER %%%;

-- FUNCTION: weather.FindClosestStation(double precision, double precision)

-- DROP FUNCTION weather."FindClosestStation"(double precision, double precision);

CREATE OR REPLACE FUNCTION FindClosestStation(
	mylatitude numeric(13,8),
	mylongitude numeric(13,8))
    RETURNS integer
    LANGUAGE 'sql'
    COST 100
    VOLATILE PARALLEL SAFE 
AS $BODY$

SELECT stationid::integer -- ,station_name, province, latitude,longitude,distance
FROM (
    SELECT stationid, latitude, longitude, abs(mylatitude-latitude)+abs(mylongitude-longitude) as distance
        ,station_name, province
    FROM weather.SK_station_coordinates SC
        INNER JOIN weather.weather_station WS ON (SC.stationid = WS.station_id::int)
    ORDER BY 4
    LIMIT 1
) L;

$BODY$;
