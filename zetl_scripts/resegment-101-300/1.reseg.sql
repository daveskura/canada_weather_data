/*
  -- Dave Skura, 2022
*/

DROP TABLE IF EXISTS canweather.new_segments;

CREATE TABLE canweather.new_segments as
SELECT *
FROM canweather.postal_code_segments
WHERE segment < 102;

INSERT INTO canweather.new_segments
SELECT province,postalcode::varchar(10),fsa::varchar(5),latitude,longitude,rnk
    ,102+((rnk/200)::integer) as segment
FROM (
    SELECT cpc.*
        ,RANK() OVER(ORDER BY cpc.postalcode,cpc.latitude,cpc.longitude,cpc.place) as rnk
    FROM canweather.canadianpostalcodes cpc -- 888687
    INNER JOIN canweather.postal_code_segments pcs USING (postalcode)
    LEFT JOIN (SELECT DISTINCT postalcode FROM canweather.dim_postal_code ) dpc USING (postalcode) -- 100999
    WHERE dpc.postalcode is null -- 787688 remaining
    ) L;

SELECT count(*) -- expect 888687 
FROM canweather.new_segments;

SELECT count(distinct segment) -- expect 4039
FROM canweather.new_segments;

SELECT segment,count(*) -- expect groupings of 1000...then 300 at 101 then 200
FROM canweather.new_segments
group by segment
ORDER BY segment;

SELECT count(*) -- should be 101298
FROM  canweather.new_segments pcs 
    INNER JOIN (SELECT DISTINCT postalcode FROM canweather.dim_postal_code ) dpc USING (postalcode) 
WHERE pcs.segment < 102;

SELECT count(*) -- should be 0
FROM  canweather.new_segments pcs 
    INNER JOIN (SELECT DISTINCT postalcode FROM canweather.dim_postal_code ) dpc USING (postalcode) 
WHERE pcs.segment > 101;

SELECT count(*) -- should be 787389
FROM  canweather.new_segments pcs 
    LEFT JOIN (SELECT DISTINCT postalcode FROM canweather.dim_postal_code ) dpc USING (postalcode) 
WHERE dpc.postalcode is null;

SELECT min(segment),max(segment) -- should be 102 and 4038
FROM  canweather.new_segments pcs 
    LEFT JOIN (SELECT DISTINCT postalcode FROM canweather.dim_postal_code ) dpc USING (postalcode) 
WHERE dpc.postalcode is null;

CREATE TABLE canweather.postal_code_segments_20230312_01 as 
SELECT *
FROM canweather.postal_code_segments;

TRUNCATE TABLE canweather.postal_code_segments;

INSERT INTO  canweather.postal_code_segments
SELECT * 
FROM canweather.new_segments;
