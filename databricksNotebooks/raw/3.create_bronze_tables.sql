-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("matchesFile", "")
-- MAGIC dbutils.widgets.text("deliveriesFile", "")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC matchesFile = dbutils.widgets.get("matchesFile")
-- MAGIC deliveriesFile = dbutils.widgets.get("deliveriesFile")
-- MAGIC print(matchesFile)
-- MAGIC print(deliveriesFile)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark.sql("""
-- MAGIC DROP TABLE IF EXISTS ipl_data_analysis_catalog.bronze.deliveries
-- MAGIC """)
-- MAGIC
-- MAGIC
-- MAGIC query = f"""
-- MAGIC CREATE TABLE IF NOT EXISTS ipl_data_analysis_catalog.bronze.deliveries (
-- MAGIC     match_id INT,
-- MAGIC     inning INT,
-- MAGIC     batting_team STRING,
-- MAGIC     bowling_team STRING,
-- MAGIC     over INT,
-- MAGIC     ball INT,
-- MAGIC     batter STRING,
-- MAGIC     bowler STRING,
-- MAGIC     non_striker STRING,
-- MAGIC     batsman_runs INT,
-- MAGIC     extra_runs INT,
-- MAGIC     total_runs INT,
-- MAGIC     extras_type STRING,
-- MAGIC     is_wicket INT,
-- MAGIC     player_dismissed STRING,
-- MAGIC     dismissal_kind STRING,
-- MAGIC     fielder STRING
-- MAGIC )
-- MAGIC USING CSV
-- MAGIC OPTIONS (
-- MAGIC     path "abfss://bronze@datalake2ipldataanalysis.dfs.core.windows.net/{deliveriesFile}",
-- MAGIC     header "true"
-- MAGIC )
-- MAGIC """
-- MAGIC
-- MAGIC spark.sql(query)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark.sql("""
-- MAGIC   DROP TABLE IF EXISTS ipl_data_analysis_catalog.bronze.matches;
-- MAGIC """)
-- MAGIC
-- MAGIC query = f"""
-- MAGIC CREATE TABLE IF NOT EXISTS ipl_data_analysis_catalog.bronze.matches (
-- MAGIC     id INT,
-- MAGIC     season STRING,
-- MAGIC     city STRING,
-- MAGIC     date DATE,
-- MAGIC     match_type STRING,
-- MAGIC     player_of_match STRING,
-- MAGIC     venue STRING,
-- MAGIC     team1 STRING,
-- MAGIC     team2 STRING,
-- MAGIC     toss_winner STRING,
-- MAGIC     toss_decision STRING,
-- MAGIC     winner STRING,
-- MAGIC     result STRING,
-- MAGIC     result_margin DOUBLE,
-- MAGIC     target_runs DOUBLE,
-- MAGIC     target_overs DOUBLE,
-- MAGIC     super_over STRING,
-- MAGIC     method STRING,
-- MAGIC     umpire1 STRING,
-- MAGIC     umpire2 STRING
-- MAGIC )
-- MAGIC USING CSV
-- MAGIC OPTIONS (
-- MAGIC   path "abfss://bronze@datalake2ipldataanalysis.dfs.core.windows.net/{matchesFile}",
-- MAGIC   header "true"
-- MAGIC )
-- MAGIC """
-- MAGIC
-- MAGIC spark.sql(query)

-- COMMAND ----------

SELECT * FROM ipl_data_analysis_catalog.bronze.matches;

-- COMMAND ----------

