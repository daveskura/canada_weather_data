@echo off
REM
REM  Dave Skura, 2023
REM

py -m zetl.postgres_export canweather.canadianpostalcodes zetl_scripts\Backup\canweather.CanadianPostalCodes.tsv 
py -m zetl.postgres_export canweather.weather_station zetl_scripts\Backup\canweather.weather_station.tsv 
py -m zetl.postgres_export canweather.station_years zetl_scripts\Backup\canweather.station_years.tsv 
py -m zetl.postgres_export canweather.station_months zetl_scripts\Backup\canweather.station_months.tsv 
py -m zetl.postgres_export canweather.station zetl_scripts\Backup\canweather.station.tsv 
py -m zetl.postgres_export canweather.station_events zetl_scripts\Backup\canweather.station_events.tsv 

