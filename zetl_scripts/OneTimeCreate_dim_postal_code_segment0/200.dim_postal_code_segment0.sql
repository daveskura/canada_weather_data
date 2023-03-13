/*
  -- Dave Skura, 2022
*/
DROP TABLE IF EXISTS canweather.dim_postal_code_tmp;

CREATE TABLE canweather.dim_postal_code_tmp as
SELECT * 
FROM (
    SELECT postalcode,caldt,closest_stationid,pc_latitude,pc_longitude,stn_latitude, stn_longitude
        ,CASE 
            WHEN closest_stationid is null THEN NULL
            WHEN closest_stationid <> LEAD(closest_stationid,1) OVER (PARTITION BY postalcode ORDER BY caldt desc) is null THEN 'eff_from_dt'
            WHEN closest_stationid is not null and LAG(closest_stationid,1) OVER (PARTITION BY postalcode ORDER BY caldt desc) is NULL THEN 'eff_to_dt'
            WHEN closest_stationid is not null and closest_stationid <> LAG(closest_stationid,1) OVER (PARTITION BY postalcode ORDER BY caldt desc) THEN 'eff_to_dt'
            WHEN closest_stationid is not null and closest_stationid <> LEAD(closest_stationid,1) OVER (PARTITION BY postalcode ORDER BY caldt desc) THEN 'eff_from_dt'
            WHEN closest_stationid is not null and closest_stationid = LAG(closest_stationid,1) OVER (PARTITION BY postalcode ORDER BY caldt desc) THEN 'Remove'
         ELSE
         closest_stationid::varchar
         END as Label
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
                FROM (  SELECT province, segment,postalcode,caldt,latitude as pc_latitude,longitude as pc_longitude -- 57410
                        FROM canweather.postal_code_segments,
                            canweather.Calendar
                        WHERE segment = 0 -- and province = 'NS' and postalcode = 'B2G2T5' -- and caldt >  '1950-01-01' -- 
                    ) A INNER JOIN (
                        SELECT province,caldt, stationid,start_dt,end_dt,latitude as stn_latitude, longitude as stn_longitude -- 15,661,595
                        FROM canweather.Station
                            INNER JOIN canweather.Calendar ON (caldt between start_dt and end_dt)
                    ) B USING (province,caldt) INNER JOIN 
                        canweather.distances
                      C ON (A.province = C.province and A.segment = C.segment and A.postalcode = C.postalcode and B.stationid = C.stationid)
                ) L
            ) M
        WHERE rnk = 1
        ) N
    ) P
WHERE label <> 'Remove'  
ORDER BY caldt desc;

CREATE INDEX ON canweather.dim_postal_code_tmp (postalcode,caldt);

DELETE FROM canweather.dim_postal_code
WHERE postalcode in (SELECT postalcode 
                    from  canweather.dim_postal_code_tmp);

INSERT INTO canweather.dim_postal_code
SELECT *
FROM (
    SELECT postalcode
        ,closest_stationid
        ,CASE WHEN label = 'eff_from_dt' THEN caldt ELSE NULL END AS eff_from_dt
        ,CASE WHEN lead(label,1) over (partition by postalcode order by caldt ) = 'eff_to_dt' THEN 
            lead(caldt,1) over (partition by postalcode order by caldt) ELSE NULL END AS eff_to_dt
    FROM canweather.dim_postal_code_tmp dpc
    ) L
WHERE eff_from_dt is not null;

/*
SELECT *
FROM canweather.dim_postal_code;
*/
