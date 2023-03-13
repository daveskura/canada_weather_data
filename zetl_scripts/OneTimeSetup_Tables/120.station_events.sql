
-- CREATE SCHEMA weather;

DROP TABLE IF EXISTS canweather.station_events;
CREATE TABLE canweather.station_events as
SELECT 
	loadfile,
	stationid::int,
	province ,
	"Latitude (y)"::float as Latitude,
	"Longitude (x)"::float as Longitude,
	"Station Name" as StationName,
	"Climate ID" as ClimateID,
	"Date/Time" as Date_Time,
	"Year"::int as Year,
	"Month"::int as Month,
	"Day"::int as Day,
	"Data Quality" as DataQuality,
	"Max Temp (C)"  as MaxTemp,
	"Max Temp Flag"  as MaxTempFlag,
	"Min Temp (C)"  as MinTemp,
	"Min Temp Flag"  as MinTempFlag,
	"Mean Temp (C)"  as MeanTemp,
	"Mean Temp Flag"  as MeanTempFlag,
	"Heat Deg Days (C)"  as HeatDegDays,
	"Heat Deg Days Flag"  as HeatDegDaysFlag,
	"Cool Deg Days (C)"  as CoolDegDays,
	"Cool Deg Days Flag"  as CoolDegDaysFlag,
	"Total Rain (mm)"  as TotalRain,
	"Total Rain Flag"  as TotalRainFlag,
	"Total Snow (cm)"  as TotalSnow,
	"Total Snow Flag"  as TotalSnowFlag,
	"Total Precip (mm)"  as TotalPrecip,
	"Total Precip Flag"  as TotalPrecipFlag,
	"Snow on Grnd (cm)"  as SnowonGrnd,
	"Snow on Grnd Flag"  as SnowonGrndFlag,
	"Dir of Max Gust (10s deg)"  as DirofMaxGust,
	"Dir of Max Gust Flag"  as DirofMaxGustFlag,
	"Spd of Max Gust (km/h)"  as SpdofMaxGust,
	"Spd of Max Gust Flag" as SpdofMaxGustFlag
FROM canweather.station_data;