/*
  -- Dave Skura, 2022
*/

SELECT COUNT(*)
FROM canweather.canadianpostalcodes;

DROP TABLE IF EXISTS canweather.TMP;
CREATE TABLE canweather.TMP AS
SELECT postalcode,provincecode as province,fsa,max(latitude) as latitude,max(longitude) as longitude,max(place) as place,fsa1,fsaprovince,areatype
FROM canweather.canadianpostalcodes cpc
    INNER JOIN canweather.postalcode_province pp ON (postalcode like concat(first_character,'%'))
GROUP BY postalcode,provincecode,fsa,fsa1,fsaprovince,areatype;

SELECT COUNT(*)
FROM canweather.TMP;

DROP TABLE canweather.canadianpostalcodes;

ALTER TABLE canweather.TMP RENAME TO canadianpostalcodes;

DROP TABLE IF EXISTS canweather.postal_code_segments;
CREATE TABLE canweather.postal_code_segments as
SELECT province,postalcode::varchar(10),fsa::varchar(5),latitude,longitude,rnk
    ,(rnk/1000)::integer as segment
FROM (
    SELECT *
    ,RANK() OVER(ORDER BY postalcode,latitude,longitude,place) as rnk
    FROM canweather.canadianpostalcodes
    ) L;

CREATE INDEX ON canweather.postal_code_segments(segment,province,postalcode);

SELECT segment,province,count(*)
FROM canweather.postal_code_segments
GROUP BY segment,province;


