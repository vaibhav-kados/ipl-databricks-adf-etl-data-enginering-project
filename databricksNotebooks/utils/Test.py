# Databricks notebook source
dbutils.widgets.text("matchesFile", "")
dbutils.widgets.text("deliveriesFile", "")

# COMMAND ----------

matchesFile = dbutils.widgets.get("matchesFile")
deliveriesFile = dbutils.widgets.get("deliveriesFile")

# COMMAND ----------

print(matchesFile)

# COMMAND ----------

print(deliveriesFile)