-- Databricks notebook source
DROP TABLE IF EXISTS ipl_data_analysis_catalog.gold.batsman_performance_sammary;

CREATE TABLE IF NOT EXISTS ipl_data_analysis_catalog.gold.batsman_performance_sammary
AS
SELECT 
   batter,
   SUM(total_runs) AS total_runs,
   COUNT(CASE WHEN extras_type != 'wides' AND extras_type != 'legbyes' AND extras_type != 'noballs' THEN 1 END) AS balls_faced,
   COUNT(CASE WHEN total_runs = 4 THEN 1 END) AS boundaries,
   COUNT(CASE WHEN total_runs = 6 THEN 1 END) AS sixes,
   ROUND(try_divide(SUM(total_runs), COUNT(CASE WHEN extras_type != 'wides' AND extras_type != 'legbyes' AND extras_type != 'noballs' THEN 1 END)) * 100,2) AS strike_rate,
   COUNT(DISTINCT match_id) AS innings
FROM ipl_data_analysis_catalog.silver.deliveries_transformed
GROUP BY batter
ORDER BY total_runs DESC
    

-- COMMAND ----------

SELECT * FROM ipl_data_analysis_catalog.gold.batsman_performance_sammary;