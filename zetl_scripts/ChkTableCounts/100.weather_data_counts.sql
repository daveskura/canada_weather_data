/*
  -- Dave Skura, 2022
*/

SELECT 'canweather.canadianpostalcodes' as src,fsaprovince,COUNT(*)
FROM canweather.canadianpostalcodes
GROUP BY fsaprovince
ORDER BY fsaprovince;

SELECT 'canweather.weather_station' as src,COUNT(*)
FROM canweather.weather_station;

SELECT 'canweather.station_years' as src,COUNT(*)
FROM canweather.station_years;

SELECT 'canweather.station_months' as src,COUNT(*)
FROM canweather.station_months;

SELECT 'canweather.station_data' as src,province,COUNT(*)
FROM canweather.station_data
GROUP BY province
ORDER BY province;

SELECT 'canweather.station' as src,COUNT(*)
FROM canweather.station;

SELECT 'canweather.station' as src,province,COUNT(*)
FROM canweather.station
GROUP BY province
ORDER BY province;

SELECT 'canweather.station_events' as src,province,COUNT(*)
FROM canweather.station_events
GROUP BY province
ORDER BY province;