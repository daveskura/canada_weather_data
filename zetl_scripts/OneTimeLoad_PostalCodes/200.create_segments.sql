/*
  -- Dave Skura, 2022
*/

SELECT COUNT(*)
FROM canweather.canadianpostalcodes;

DROP TABLE IF EXISTS canweather.TMP;
CREATE TABLE canweather.TMP AS
SELECT postalcode,fsa,max(latitude) as latitude,max(longitude) as longitude,max(place) as place,fsa1,fsaprovince,areatype
FROM canweather.canadianpostalcodes
GROUP BY postalcode,fsa,fsa1,fsaprovince,areatype;

SELECT COUNT(*)
FROM canweather.TMP;

TRUNCATE TABLE canweather.canadianpostalcodes;

INSERT INTO canweather.canadianpostalcodes
SELECT *
FROM canweather.TMP;


DROP TABLE IF EXISTS canweather.postal_code_segments;
CREATE TABLE canweather.postal_code_segments as
SELECT postalcode::varchar(10),fsa::varchar(5),latitude,longitude,place,fsa1,fsaprovince,areatype,rnk
    ,(rnk/20000)::integer as segment
FROM (
    SELECT *
    ,RANK() OVER(ORDER BY postalcode,latitude,longitude,place) as rnk
    FROM canweather.canadianpostalcodes
    ) L;

CREATE INDEX ON canweather.postal_code_segments(segment,postalcode);

SELECT segment,count(*)
FROM canweather.postal_code_segments
GROUP BY segment;


