# Goverance Area

This area of the repo contains the objects used for etl flow, quality checks, and auditing. No raw data flows from these objects into the reports. Unlike the scripts/functions in the `common` folder, these objects are more metadata related. 
---

## Set_Load_Master

### Overview

This is an utility module to assist in establishing the date range of data to be loaded into the bronze layer. By default the date range is the last 45 days. The whole history of data can be retrieved by adjusting this script if required. All history is kept in the source files, stored in ADLS2. By default, the bronze tables in databricks only contain data for the last 45 days. This is to reduce data processing times and costs when loading data into bronze tables, and inserting this into the silver layer. The silver layer is incrementally loaded, so it contains data over 45 days old. 