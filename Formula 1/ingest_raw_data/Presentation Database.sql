-- Databricks notebook source
use catalog hive_metastore;
select current_catalog()

-- COMMAND ----------

-- DBTITLE 1,Create Presentation Database
CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION '/mnt/f1datastoragelake/f1_presentation'

-- COMMAND ----------

desc database f1_presentation

-- COMMAND ----------


