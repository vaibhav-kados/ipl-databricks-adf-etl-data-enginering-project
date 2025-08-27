-- Databricks notebook source
CREATE EXTERNAL LOCATION IF NOT EXISTS external_adls_bronze
    URL "abfss://bronze@datalake2ipldataanalysis.dfs.core.windows.net"
    WITH (STORAGE CREDENTIAL `external_adls_credentials`)

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS external_adls_silver
    URL "abfss://silver@datalake2ipldataanalysis.dfs.core.windows.net"
    WITH (STORAGE CREDENTIAL `external_adls_credentials`)

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS external_adls_gold
    URL "abfss://gold@datalake2ipldataanalysis.dfs.core.windows.net"
    WITH (STORAGE CREDENTIAL `external_adls_credentials`)