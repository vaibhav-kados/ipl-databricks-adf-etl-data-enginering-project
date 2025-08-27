-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS ipl_data_analysis_catalog 
MANAGED LOCATION 'abfss://bronze@datalake2ipldataanalysis.dfs.core.windows.net';

-- COMMAND ----------

USE CATALOG ipl_data_analysis_catalog;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS bronze
MANAGED LOCATION  "abfss://bronze@datalake2ipldataanalysis.dfs.core.windows.net"

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS silver
MANAGED LOCATION "abfss://silver@datalake2ipldataanalysis.dfs.core.windows.net"

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS gold
MANAGED LOCATION "abfss://gold@datalake2ipldataanalysis.dfs.core.windows.net"

-- COMMAND ----------

SHOW SCHEMAS

-- COMMAND ----------

