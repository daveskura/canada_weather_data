CREATE TABLE IF NOT EXISTS canweather.weather_station(
	station_id integer 		/* eg. 10700 */ ,
	station_name text 		/* eg. (AE) BOW SUMMIT */ ,
	province text 		/* eg. AB */ ,
	webid text 		/* eg. stnRequest0 */ 
);

COMMENT ON TABLE canweather.weather_station IS 'This Postgres table was defined by schemawiz for loading the csv file c:\git\canada_weather_data\zetl_scripts\Restore\canweather.weather_station.tsv, delimiter (	)';
