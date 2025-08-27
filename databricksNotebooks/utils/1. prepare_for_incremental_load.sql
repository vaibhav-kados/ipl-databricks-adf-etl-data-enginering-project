-- Databricks notebook source
USE CATALOG ipl_data_analysis_catalog

-- COMMAND ----------

DROP SCHEMA IF EXISTS ipl_data_analysis_catalog.gold;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ipl_data_analysis_catalog.gold;