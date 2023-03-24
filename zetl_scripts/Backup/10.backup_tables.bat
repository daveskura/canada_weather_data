@echo off
REM
REM  Dave Skura, 2023
REM
REM EXPORT 

py -m zetl.postgres_export canweather.Calendar zetl_scripts\Backup\canweather.Calendar.tsv 
py -m zetl.postgres_export canweather.postal_code_segments zetl_scripts\Backup\canweather.postal_code_segments.tsv 
REM py -m zetl.postgres_export canweather.Station zetl_scripts\Backup\canweather.Station.tsv 
REM py -m zetl.postgres_export canweather.distances zetl_scripts\Backup\canweather.distances.tsv 
REM py -m zetl.postgres_export canweather.dim_postal_code zetl_scripts\Backup\canweather.dim_postal_code.tsv 

