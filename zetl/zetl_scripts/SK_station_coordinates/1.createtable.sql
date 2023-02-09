/*
  -- Dave Skura, 2022
*/

DROP TABLE IF EXISTS weather.SK_station_coordinates;

CREATE TABLE weather.SK_station_coordinates as 
SELECT DISTINCT stationid::int,"Latitude (y)"::float as latitude,"Longitude (x)"::float as longitude
FROM station_data;

SELECT COUNT(*)
FROM weather.SK_station_coordinates;