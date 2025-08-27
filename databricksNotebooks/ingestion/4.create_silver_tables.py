# Databricks notebook source
# MAGIC %run "../includes/common_functions"
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------


dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------


dbutils.widgets.text("v_file_date","")
v_file_date = dbutils.widgets.get("v_file_date")

# COMMAND ----------

from pyspark.sql.functions import col, coalesce, lit, year

matches_source_df = spark.read.format("delta").table("ipl_data_analysis_catalog.bronze.matches") \
    .withColumnRenamed("id", "match_id") \
    .withColumn("date", col("date").cast("date")) \
    .withColumn("result_margin", coalesce(col("result_margin"), lit(0))) \
    .withColumn("method", coalesce(col("method"), lit("NA"))) \
    .withColumn("match_year", year(col("date")))

# COMMAND ----------


import pandas as pd
from pyspark.sql.functions import lit


# COMMAND ----------

matches_df_with_timestamp = matches_source_df.withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

df_with_ingestion_date = add_ingestion_date(matches_df_with_timestamp)

# COMMAND ----------

merge_condition1 = """tgt.match_id = src.match_id"""

merge_delta_data(df_with_ingestion_date, "ipl_data_analysis_catalog.silver", "matches_transformed" , silver_folder_path, merge_condition1)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM ipl_data_analysis_catalog.silver.matches_transformed

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT MIN(match_id) FROM ipl_data_analysis_catalog.silver.matches_transformed;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT MAX(match_id) FROM ipl_data_analysis_catalog.silver.matches_transformed;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM ipl_data_analysis_catalog.silver.matches_transformed;

# COMMAND ----------

# df_with_ingestion_date.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable("ipl_data_analysis_catalog.silver.matches_transformed")

# COMMAND ----------

# %sql
# -- DROP TABLE IF EXISTS ipl_data_analysis_catalog.silver.deliveries_transformed;

# CREATE TABLE IF NOT EXISTS ipl_data_analysis_catalog.silver.deliveries_transformed
# AS
# SELECT
#     match_id,
#     inning,
#     batting_team,
#     bowling_team,
#     over,
#     ball,
#     batter,
#     bowler,
#     non_striker,
#     batsman_runs,
#     extra_runs,
#     total_runs,
#     COALESCE(extras_type, 'None') AS extras_type,
#     is_wicket,
#     player_dismissed,
#     COALESCE(dismissal_kind, 'None') AS dismissal_kind,
#     COALESCE(fielder, 'None') AS fielder,
#     -- New Derived Columns
#     CASE WHEN extras_type IS NOT NULL THEN 1 ELSE 0 END AS is_extra,
#     CASE WHEN batsman_runs >= 4 THEN 1 ELSE 0 END AS is_boundary,
#     current_timestamp() AS ingestion_date
# FROM
#     ipl_data_analysis_catalog.bronze.deliveries;

# COMMAND ----------

from pyspark.sql.functions import col, coalesce, lit, year, when, current_timestamp

deliveries_source_df = spark.read.format("delta").table("ipl_data_analysis_catalog.bronze.deliveries") \
    .withColumn("extras_type", coalesce(col("extras_type"), lit("None"))) \
    .withColumn("dismissal_kind", coalesce(col("dismissal_kind"), lit("None"))) \
    .withColumn("fielder", coalesce(col("fielder"), lit("None"))) \
    .withColumn("is_extra", when(col("extras_type") != "None", lit(1)).otherwise(lit(0))) \
    .withColumn("is_boundary", when(col("batsman_runs") >= 4, lit(1)).otherwise(lit(0))) 

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import lit


# deliveries_transformed_df = spark.read.format("delta").table("ipl_data_analysis_catalog.silver.deliveries_transformed")

# COMMAND ----------

deliveries_df_with_timestamp = deliveries_source_df.withColumn("data_source", lit(v_data_source)).withColumn("file_date",lit(v_file_date))
       
df_final_deliveries = add_ingestion_date(deliveries_df_with_timestamp)

# COMMAND ----------

# df_with_ingestion_date.write.mode("overwrite").option("mergeSchema","true").format("delta").saveAsTable("ipl_data_analysis_catalog.silver.deliveries_transformed")

# COMMAND ----------

merge_condition2 = """
tgt.match_id = src.match_id
AND tgt.inning = src.inning
AND tgt.over = src.over
AND tgt.ball = src.ball
"""

merge_delta_data(df_final_deliveries, "ipl_data_analysis_catalog.silver", "deliveries_transformed" , silver_folder_path, merge_condition2, 'match_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE ipl_data_analysis_catalog.silver.deliveries_transformed

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT MIN(match_id) FROM ipl_data_analysis_catalog.silver.deliveries_transformed;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT MAX(match_id) FROM ipl_data_analysis_catalog.silver.deliveries_transformed; 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM ipl_data_analysis_catalog.silver.deliveries_transformed LIMIT 100;