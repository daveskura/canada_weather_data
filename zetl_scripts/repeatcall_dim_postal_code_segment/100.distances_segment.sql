/*
  -- Dave Skura, 2022
*/

DROP TABLE IF EXISTS canweather.distances cascade;

CREATE TABLE canweather.distances as 
SELECT province,segment,postalcode,stationid,
    distance + (rnk * 0.0000000001) as distance 
FROM (
    SELECT *
        ,RANK() OVER (PARTITION BY province,segment,postalcode,distance ORDER BY stationid) as rnk
    FROM (
        SELECT province,segment,PC.postalcode,stationid,
            abs(PC.latitude::numeric(13,8)-SC.latitude::numeric(13,8))+abs(PC.longitude::numeric(13,8)-SC.longitude::numeric(13,8)) as distance
        FROM canweather.postal_code_segments PC
            INNER JOIN canweather.Station SC USING (province)
        WHERE segment=<ARGV1>
        ) L
    ) M
WHERE segment=<ARGV1>;

CREATE INDEX ON canweather.distances(province,segment,postalcode,stationid);

SELECT COUNT(*)
FROM canweather.distances;

SELECT province,segment,postalcode,distance,count(*)
FROM canweather.distances
WHERE segment=<ARGV1> -- and postalcode = 'B2G2T5' -- and stationid in (6463,6500)
GROUP BY province,segment,postalcode,distance
HAVING COUNT(*) > 1;
