# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column=None):
    from delta.tables import DeltaTable
    
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")

    if spark.catalog.tableExists(f"{db_name}.{table_name}"):
        deltaTable = DeltaTable.forName(spark, f"{db_name}.{table_name}")
        deltaTable.alias("tgt").merge(
            input_df.alias("src"),
            merge_condition
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    else:
        writer = input_df.write.mode("overwrite").format("delta")
        if partition_column:   # only partition if provided
            writer = writer.partitionBy(partition_column)
        writer.saveAsTable(f"{db_name}.{table_name}")
