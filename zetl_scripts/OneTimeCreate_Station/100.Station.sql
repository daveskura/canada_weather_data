/*
  -- Dave Skura, 2022
*/

DROP TABLE IF EXISTS canweather.Station;

CREATE TABLE canweather.Station as 
SELECT 
	stationid
	,StationName
	,province
	,Latitude
	,Longitude
	,min(to_date(date_time,'YYYY-MM-DD')) as start_dt
	,max(to_date(date_time,'YYYY-MM-DD')) as end_dt
FROM canweather.station_events
GROUP BY stationid,StationName,province,Latitude,Longitude;
