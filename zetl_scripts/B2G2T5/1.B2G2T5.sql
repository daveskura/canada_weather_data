/*
  -- Dave Skura, 2022
*/
SELECT segment,COUNT(*)
FROM canweather.postal_code_segments
WHERE segment = <ARGV1>
GROUP BY segment;


SELECT *
FROM canweather.postal_code_segments
WHERE segment = <ARGV1>
limit 5;
