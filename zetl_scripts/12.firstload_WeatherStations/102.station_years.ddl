/*
-- CREATE SCHEMA canweather;
*/

DROP TABLE if exists canweather.station_years ;
CREATE TABLE canweather.station_years (
	station_id varchar(25),
	year integer
);
