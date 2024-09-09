-- Databricks notebook source
use catalog hive_metastore;
use database f1_presentation;

-- COMMAND ----------

desc table calculated_race_results

-- COMMAND ----------

SELECT
  team_name,
  count(*) AS total_race,
  sum(calculated_position) AS total_points
  FROM calculated_race_results
  GROUP BY team_name
  ORDER BY total_points DESC

-- COMMAND ----------


