/*
  -- Dave Skura, 2022
*/

SELECT COUNT(*) as  total_rowcount
FROM canweather.station_events;

SELECT COUNT(*) as bad_rowcount
FROM canweather.station_events
WHERE maxtemp = '' and mintemp = '' and meantemp = '' and spdofmaxgust = '' and totalprecip = ''; 

DELETE  
FROM canweather.station_events 
WHERE maxtemp = '' and mintemp = '' and meantemp = '' and spdofmaxgust = '' and totalprecip = ''; 

SELECT COUNT(*) as  total_rowcount
FROM canweather.station_events;
