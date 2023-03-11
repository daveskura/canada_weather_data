CREATE TABLE IF NOT EXISTS CanadianPostalCodes(
	postalcode text 		/* eg. S4P1W4 */ ,
	fsa text 		/* eg. S4P */ ,
	latitude numeric 		/* eg. 50.44637 */ ,
	longitude numeric 		/* eg. -104.605044 */ ,
	place text 		/* eg. Regina */ ,
	fsa1 text 		/* eg. S */ ,
	fsaprovince integer 		/* eg. 47 */ ,
	areatype text 		/* eg. Urban */ 
);

COMMENT ON TABLE CanadianPostalCodes IS 'This Postgres table was defined by schemawiz for loading the csv file c:\git\canada_weather_data\zetl_scripts\Restore\canweather.CanadianPostalCodes.tsv, delimiter (	)';
