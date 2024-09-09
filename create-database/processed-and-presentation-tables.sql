-- Databricks notebook source
use catalog hive_metastore;

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/f1datastoragelake/processed";

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation 
LOCATION "/mnt/f1datastoragelake/presentation";

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_raw CASCADE;

-- COMMAND ----------


