CREATE TABLE IF NOT EXISTS canweather.station_months(
	station_id integer 		/* eg. 10700 */ ,
	month integer 		/* eg. 1 */ 
);

COMMENT ON TABLE canweather.station_months IS 'This Postgres table was defined by schemawiz for loading the csv file c:\git\canada_weather_data\zetl_scripts\Restore\canweather.station_months.tsv, delimiter (	)';
