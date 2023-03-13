DROP TABLE IF EXISTS canweather.station;
CREATE TABLE IF NOT EXISTS canweather.station(
	stationid integer 		/* eg. 5 */ ,
	stationname text 		/* eg. SAANICH OLDFIELD NORTH */ ,
	province text 		/* eg. BC */ ,
	latitude numeric 		/* eg. 48.55 */ ,
	longitude numeric 		/* eg. -123.42 */ 
);

COMMENT ON TABLE canweather.station IS 'This Postgres table was defined by schemawiz for loading the csv file c:\git\canada_weather_data\zetl_scripts\Restore\canweather.station.tsv, delimiter (	)';
