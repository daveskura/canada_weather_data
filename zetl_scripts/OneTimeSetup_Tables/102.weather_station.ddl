/*
-- CREATE SCHEMA canweather;

*/
DROP TABLE if exists canweather.weather_station;
CREATE TABLE canweather.weather_station (
	station_id varchar(25) primary key,
	station_name varchar(250),
	province varchar(50),
	webid varchar(50)
);

