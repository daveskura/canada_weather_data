/*
  -- Dave Skura, 2022
*/

DROP TABLE IF EXISTS canweather.dim_postal_test;

CREATE TABLE canweather.dim_postal_test as 
SELECT postalcode,caldt,stationid as closest_stationid -- ,pc_latitude,pc_longitude,stn_latitude, stn_longitude
FROM (
	SELECT  postalcode
		, caldt
		, stationid
		-- ,pc_latitude,pc_longitude,stn_latitude, stn_longitude
		, rank() OVER (PARTITION BY postalcode,caldt ORDER BY distance) as rnk
	FROM (
	
		SELECT A.postalcode::varchar(10),A.caldt,B.stationid,C.distance -- ,pc_latitude,pc_longitude,stn_latitude, stn_longitude
		FROM (  SELECT segment,postalcode,caldt -- ,latitude as pc_latitude,longitude as pc_longitude -- 57410
				FROM canweather.postal_code_segments,
					canweather.Calendar
				WHERE segment = 1 and postalcode = 'B2G2T5' -- and caldt < '1950-04-09'
			) A INNER JOIN (
				SELECT caldt, stationid -- ,start_dt,end_dt,latitude as stn_latitude, longitude as stn_longitude -- 15,661,595
				FROM canweather.Station
					INNER JOIN canweather.Calendar ON (caldt between start_dt and end_dt)
			) B ON (A.caldt = B.caldt) INNER JOIN 
				canweather.distances
			  C ON (A.segment = C.segment and A.postalcode = C.postalcode and B.stationid = C.stationid)
		) L
	) M
WHERE rnk = 1;

CREATE INDEX ON canweather.dim_postal_test(closest_stationid,caldt);

SELECT count(*) 
FROM (
    
    SELECT postalcode,caldt,closest_stationid -- ,pc_latitude,pc_longitude,stn_latitude, stn_longitude
        ,CASE 
            WHEN closest_stationid is null THEN NULL
            WHEN closest_stationid <> LAG(closest_stationid,1) OVER (ORDER BY caldt) is null THEN 'eff_from_dt'
            WHEN closest_stationid is not null and LEAD(closest_stationid,1) OVER (ORDER BY caldt) is NULL THEN 'eff_to_dt'
            WHEN closest_stationid is not null and closest_stationid <> LEAD(closest_stationid,1) OVER (ORDER BY caldt) THEN 'eff_to_dt'
            WHEN closest_stationid is not null and closest_stationid <> LAG(closest_stationid,1) OVER (ORDER BY caldt ) THEN 'eff_from_dt'
            WHEN closest_stationid is not null and closest_stationid = LEAD(closest_stationid,1) OVER (ORDER BY caldt) THEN 'Remove'
         ELSE
         closest_stationid::varchar
         END as Label
    FROM canweather.dim_postal_test
        
    ) P
WHERE label <> 'Remove'  ;
-- ORDER BY caldt 
