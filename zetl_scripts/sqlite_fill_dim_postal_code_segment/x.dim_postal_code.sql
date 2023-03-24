/*
  -- Dave Skura, 2022
*/

DROP TABLE IF EXISTS dim_postal_code_tmp ;

CREATE TABLE dim_postal_code_tmp as
WITH dim_postal_code_tmp as (
    WITH distances AS (  
        SELECT province,segment,postalcode,stationid,
            distance + (rnk * 0.0000000001) as distance 
        FROM (
            SELECT *
                ,RANK() OVER (PARTITION BY province,segment,postalcode,distance ORDER BY stationid) as rnk
            FROM (
                SELECT province,segment,PC.postalcode,stationid,
                    abs(PC.latitude-SC.latitude)+abs(PC.longitude-SC.longitude) as distance
                FROM postal_code_segments PC
                    INNER JOIN Station SC USING (province)
                WHERE postalcode = 'G1E4W1' 
                ) L
            ) M
        WHERE postalcode = 'G1E4W1'
    )
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
             closest_stationid
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
                    FROM (  SELECT province, segment,postalcode,caldt,latitude as pc_latitude,longitude as pc_longitude 
                            FROM postal_code_segments,
                                Calendar
                            WHERE postalcode = 'G1E4W1'
                        ) A INNER JOIN (
                            SELECT province,caldt, stationid,start_dt,end_dt,latitude as stn_latitude, longitude as stn_longitude 
                            FROM Station
                                INNER JOIN Calendar ON (caldt between start_dt and end_dt)
                        ) B USING (province,caldt) INNER JOIN 
                            distances
                          C ON (A.province = C.province and A.segment = C.segment and A.postalcode = C.postalcode and B.stationid = C.stationid)
                    ) L
                ) M
            WHERE rnk = 1
            ) N
        ) P
    WHERE label <> 'Remove'  
    ORDER BY caldt desc
 )
 
SELECT *
FROM (
    SELECT postalcode
        ,closest_stationid
        ,CASE WHEN label = 'eff_from_dt' THEN caldt ELSE NULL END AS eff_from_dt
        ,CASE WHEN lead(label,1) over (partition by postalcode order by caldt ) = 'eff_to_dt' THEN 
            lead(caldt,1) over (partition by postalcode order by caldt) ELSE NULL END AS eff_to_dt
    FROM dim_postal_code_tmp dpc
    ) L
WHERE eff_from_dt is not null;


DELETE FROM dim_postal_code
WHERE postalcode = 'G1E4W1';

INSERT INTO dim_postal_code
SELECT *
FROM dim_postal_code_tmp;

DROP TABLE IF EXISTS dim_postal_code_tmp;