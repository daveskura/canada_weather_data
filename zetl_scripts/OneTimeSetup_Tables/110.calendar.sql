/*
  -- Dave Skura, 2022
*/
DROP TABLE IF EXISTS canweather.calendar;

CREATE TABLE canweather.calendar as
SELECT 
    EXTRACT(year FROM caldt) as year
    ,EXTRACT(month FROM caldt) as month
    ,EXTRACT(day FROM caldt) as day
    ,caldt
FROM (
    SELECT  date(CURRENT_DATE - (n || ' day')::INTERVAL) as caldt
    FROM    generate_series(0, 25638) n
      ) Cal;

CREATE UNIQUE INDEX ON canweather.calendar(caldt);