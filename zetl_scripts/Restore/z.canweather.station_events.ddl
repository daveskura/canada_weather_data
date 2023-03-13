DROP TABLE IF EXISTS canweather.station_events;
CREATE TABLE IF NOT EXISTS canweather.station_events(
	loadfile text 		/* eg. 10892_QC_1995_12.csv */ ,
	stationid integer 		/* eg. 10892 */ ,
	province text 		/* eg. QC */ ,
	latitude numeric 		/* eg. 45.31 */ ,
	longitude numeric 		/* eg. -72.24 */ ,
	stationname text 		/* eg. MONT-ORFORD */ ,
	climateid text 		/* eg. 7025229 */ ,
	date_time date 		/* eg. 1995-10-12 */ ,
	year integer 		/* eg. 1995 */ ,
	month integer 		/* eg. 10 */ ,
	day integer 		/* eg. 12 */ ,
	dataquality text 		/* eg.  */ ,
	maxtemp numeric 		/* eg. 16.9 */ ,
	maxtempflag text 		/* eg.  */ ,
	mintemp numeric 		/* eg. 8.8 */ ,
	mintempflag text 		/* eg.  */ ,
	meantemp numeric 		/* eg. 12.9 */ ,
	meantempflag text 		/* eg.  */ ,
	heatdegdays numeric 		/* eg. 5.1 */ ,
	heatdegdaysflag text 		/* eg.  */ ,
	cooldegdays numeric 		/* eg. 0.0 */ ,
	cooldegdaysflag text 		/* eg.  */ ,
	totalrain text 		/* eg.  */ ,
	totalrainflag text 		/* eg. M */ ,
	totalsnow text 		/* eg.  */ ,
	totalsnowflag text 		/* eg. M */ ,
	totalprecip numeric 		/* eg. 0.0 */ ,
	totalprecipflag text 		/* eg.  */ ,
	snowongrnd text 		/* eg.  */ ,
	snowongrndflag text 		/* eg.  */ ,
	dirofmaxgust integer 		/* eg. 26 */ ,
	dirofmaxgustflag text 		/* eg.  */ ,
	spdofmaxgust text 		/* eg. 65 */ ,
	spdofmaxgustflag text 		/* eg. M */ 
);

COMMENT ON TABLE canweather.station_events IS 'This Postgres table was defined by schemawiz for loading the csv file c:\git\canada_weather_data\zetl_scripts\Restore\canweather.station_events.tsv, delimiter (	)';
COMMENT ON COLUMN canweather.station_events.date_time IS 'date format in csv [YYYY/MM/DD]';
