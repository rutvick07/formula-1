-- Databricks notebook source
use catalog hive_metastore;
use database f1_presentation;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Queries to perform analysis on the most Dominat Drivers.

-- COMMAND ----------

select * from f1_presentation.calculated_race_results where driver_id = 1

-- COMMAND ----------

desc table calculated_race_results;

-- COMMAND ----------

SELECT 
  driver_name,
  count(*) AS total_races,
  avg(calculated_position) AS avg_points,
  sum(calculated_position) AS total_points  
  FROM calculated_race_results
  WHERE race_year BETWEEN 2010 AND 2020
  GROUP BY driver_name
  HAVING total_races > 50
  ORDER BY avg_points DESC;

-- COMMAND ----------

SELECT 
  driver_name,
  count(*) AS total_races,
  avg(calculated_points) AS avg_points,
  sum(calculated_points) AS total_points  
  FROM calculated_race_results
  WHERE race_year BETWEEN 2010 AND 2020
  GROUP BY driver_name
  HAVING total_races > 50
  ORDER BY avg_points DESC;

-- COMMAND ----------


