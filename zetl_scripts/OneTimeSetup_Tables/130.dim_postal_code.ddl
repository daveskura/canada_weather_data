/*
  -- Dave Skura, 2022
*/

DROP TABLE IF EXISTS canweather.dim_postal_code;
CREATE TABLE canweather.dim_postal_code (
    postalcode	varchar(12),
    closest_stationid	integer,
    eff_from_dt	date,
    eff_to_dt	date
);