@echo off
REM
REM  Dave Skura, 2023
REM

py -m zetl.postgres_import zetl_scripts\10.firstload_PostalCodes\CanadianPostalCodes.csv canweather.canadianpostalcodes True