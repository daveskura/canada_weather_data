/*
  -- Dave Skura, 2022
*/

DROP TABLE IF EXISTS canweather.postalcode_province;

CREATE TABLE canweather.postalcode_province (
	First_Character char(1),
	ProvinceCode char(2),
	ProvinceName varchar(75)
);

INSERT INTO canweather.postalcode_province (First_Character,ProvinceCode,ProvinceName) VALUES
('A','NL','Newfoundland and Labrador'),
('B','NS','Nova Scotia'),
('C','PE','Prince Edward Island'),
('E','NB','New Brunswick'),
('G','QC','Eastern Quebec'),
('H','QC','Metropolitan Montréal'),
('J','QC','Western Quebec'),
('K','ON','Eastern Ontario'),
('L','ON','Central Ontario'),
('M','ON','Metropolitan Toronto'),
('N','ON','Southwestern Ontario'),
('P','ON','Northern Ontario'),
('R','MB','Manitoba'),
('S','SK','Saskatchewan'),
('T','AB','Alberta'),
('V','BC','British Columbia'),
('X','NT','Northwest Territories and Nunavut'),
('Y','YT','Yukon');