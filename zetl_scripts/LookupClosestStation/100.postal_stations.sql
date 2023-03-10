/*
  -- Dave Skura, 2022


SELECT postalcode, fsa, latitude, longitude, place, fsa1, fsaprovince, areatype 
FROM postal_codes PC
    
WHERE fsa like 'S%' and postalcode='S4N0B2' -- SK postal codes

*/
DROP TABLE IF EXISTS canweather.postal_stations;

CREATE TABLE canweather.postal_stations as 
SELECT PC.*,S.*
FROM canweather.canadianpostalcodes PC
    INNER JOIN canweather.Station S ON (FindClosestStation(PC.latitude::numeric,PC.longitude::numeric) = S.stationid)
WHERE PC.postalcode = 'L5L3B1' -- PC.FSA like 'L5L';

SELECT *
FROM canweather.postal_stations
limit 15;

		
