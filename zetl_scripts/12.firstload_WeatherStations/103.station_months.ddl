/*
-- CREATE SCHEMA canweather;
*/
DROP TABLE if exists canweather.station_months ;

CREATE TABLE canweather.station_months (
	station_id varchar(25),
	month integer
);

