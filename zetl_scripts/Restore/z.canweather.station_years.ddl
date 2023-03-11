CREATE TABLE IF NOT EXISTS canweather.station_years(
	station_id integer 		/* eg. 10700 */ ,
	year integer 		/* eg. 1998 */ 
);

COMMENT ON TABLE canweather.station_years IS 'This Postgres table was defined by schemawiz for loading the csv file c:\git\canada_weather_data\zetl_scripts\Restore\canweather.station_years.tsv, delimiter (	)';
