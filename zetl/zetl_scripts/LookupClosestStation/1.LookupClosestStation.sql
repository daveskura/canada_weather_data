/*
  -- Dave Skura, 2022


SELECT postalcode, fsa, latitude, longitude, place, fsa1, fsaprovince, areatype 
FROM postal_codes PC
    
WHERE fsa like 'S%' and postalcode='S4N0B2' -- SK postal codes

*/
DROP TABLE IF EXISTS weather.postal_station_sk;

CREATE TABLE weather.postal_station_sk as 
SELECT PC.*,public.FindClosestStation(latitude::numeric,longitude::numeric) as closest_weather_station
FROM weather.postal_codes PC
WHERE PC.FSA like 'S%';

SELECT *
FROM weather.postal_station_sk
limit 15;

		
