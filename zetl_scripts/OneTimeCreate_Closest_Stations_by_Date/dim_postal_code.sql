/*
  -- Dave Skura, 2022
*/
DROP TABLE IF EXISTS canweather.dim_postal_code;

CREATE TABLE canweather.dim_postal_code as
SELECT * 
FROM (
    SELECT postalcode,caldt,closest_stationid,pc_latitude,pc_longitude,stn_latitude, stn_longitude
        ,CASE 
            WHEN closest_stationid is null THEN NULL
            WHEN closest_stationid <> LEAD(closest_stationid,1) OVER (ORDER BY caldt desc) is null THEN 'eff_from_dt'
            WHEN closest_stationid is not null and LEAD(closest_stationid,1) OVER (ORDER BY caldt) is NULL THEN 'eff_to_dt'
            WHEN closest_stationid is not null and closest_stationid <> LEAD(closest_stationid,1) OVER (ORDER BY caldt) THEN 'eff_to_dt'
            WHEN closest_stationid is not null and closest_stationid <> LEAD(closest_stationid,1) OVER (ORDER BY caldt desc) THEN 'eff_from_dt'
            WHEN closest_stationid is not null and closest_stationid = LEAD(closest_stationid,1) OVER (ORDER BY caldt) THEN 'Remove'
         ELSE
         closest_stationid::varchar
         END as Label
         ,LEAD(closest_stationid,1) OVER (ORDER BY caldt )
         ,LEAD(closest_stationid,1) OVER (ORDER BY caldt desc)
    FROM (
        SELECT postalcode,caldt,pc_latitude,pc_longitude,stn_latitude, stn_longitude,stationid as closest_stationid
        FROM (
            SELECT  postalcode
                , stationid
                , caldt
                ,pc_latitude,pc_longitude,stn_latitude, stn_longitude
                , rank() OVER (PARTITION BY postalcode,caldt ORDER BY distance) as rnk
            FROM (
            
                SELECT A.postalcode,B.stationid,A.caldt,C.distance,pc_latitude,pc_longitude,stn_latitude, stn_longitude
                FROM (  SELECT segment,postalcode,caldt,latitude as pc_latitude,longitude as pc_longitude -- 57410
                        FROM canweather.postal_code_segments,
                            canweather.Calendar
                        WHERE segment = 1 and postalcode = 'B2G2T5' -- and caldt >  '1950-01-01' -- 
                    ) A INNER JOIN (
                        SELECT caldt, stationid,start_dt,end_dt,latitude as stn_latitude, longitude as stn_longitude -- 15,661,595
                        FROM canweather.Station
                            INNER JOIN canweather.Calendar ON (caldt between start_dt and end_dt)
                    ) B ON (A.caldt = B.caldt) INNER JOIN 
                        canweather.distances
                      C ON (A.segment = C.segment and A.postalcode = C.postalcode and B.stationid = C.stationid)
                ) L
            ) M
        WHERE rnk = 1
        ) N
    ) P
WHERE label <> 'Remove'  
ORDER BY caldt desc;

