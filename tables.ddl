
CREATE SCHEMA weather;

DROP TABLE weather.weather_station;
CREATE TABLE weather.weather_station (
	station_id varchar(25) primary key,
	station_name varchar(250),
	province varchar(50),
	webid varchar(50)
);
DROP TABLE weather.station_years ;
CREATE TABLE weather.station_years (
	station_id varchar(25),
	year integer
);
DROP TABLE weather.station_months ;

CREATE TABLE weather.station_months (
	station_id varchar(25),
	month integer
);

TRUNCATE TABLE weather.weather_station;
TRUNCATE TABLE weather.station_years;
TRUNCATE TABLE weather.station_months;

DROP TABLE weather.station_data;
CREATE TABLE weather.station_data (
	"Longitude (x)" varchar(10),
	"Latitude (y)" varchar(10),
	"Station Name" varchar(250),
	"Climate ID" varchar(25),
	"Date/Time" varchar(25),
	"Year" varchar(10),
	"Month" varchar(10),
	"Day" varchar(10),
	"Data Quality" varchar(25),
	"Max Temp (C)"  varchar(25),
	"Max Temp Flag"  varchar(25),
	"Min Temp (C)"  varchar(25),
	"Min Temp Flag"  varchar(25),
	"Mean Temp (C)"  varchar(25),
	"Mean Temp Flag"  varchar(25),
	"Heat Deg Days (C)"  varchar(25),
	"Heat Deg Days Flag"  varchar(25),
	"Cool Deg Days (C)"  varchar(25),
	"Cool Deg Days Flag"  varchar(25),
	"Total Rain (mm)"  varchar(25),
	"Total Rain Flag"  varchar(25),
	"Total Snow (cm)"  varchar(25),
	"Total Snow Flag"  varchar(25),
	"Total Precip (mm)"  varchar(25),
	"Total Precip Flag"  varchar(25),
	"Snow on Grnd (cm)"  varchar(25),
	"Snow on Grnd Flag"  varchar(25),
	"Dir of Max Gust (10s deg)"  varchar(25),
	"Dir of Max Gust Flag"  varchar(25),
	"Spd of Max Gust (km/h)"  varchar(25),
	"Spd of Max Gust Flag" varchar(25)
);