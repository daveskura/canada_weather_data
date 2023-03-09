CREATE TABLE IF NOT EXISTS canweather.canadianpostalcodes(
	PostalCode text 		/* eg. A0A0A0 */ ,
	FSA text 		/* eg. A0A */ ,
	Latitude numeric 		/* eg. 48.56745 */ ,
	Longitude numeric 		/* eg. -54.843225 */ ,
	Place text 		/* eg. Gander */ ,
	FSA1 text 		/* eg. A */ ,
	FSAProvince integer 		/* eg. 10 */ ,
	AreaType text 		/* eg. Rural */ 
);

COMMENT ON TABLE canweather.canadianpostalcodes IS 'This Postgres table was defined by schemawiz for loading the csv file zetl_scripts\10.firstload_PostalCodes\CanadianPostalCodes.csv, delimiter (,)';
