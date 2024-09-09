# Databricks notebook source
# MAGIC %run "../includes/config_variables"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hive_metastore

# COMMAND ----------

# Reading 'processed/drivers' delta table.
driversDf = spark.read.format("delta").load(f"{processed_absolute}/drivers") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality") 

# COMMAND ----------

# Reading 'processed/circuits' delta table.
circuitsDf = spark.read.format("delta").load(f"{processed_absolute}/circuits") \
.withColumnRenamed("location", "circuit_location") 

# COMMAND ----------

# Reading 'processed/races' delta table.
racesDf = spark.read.format("delta").load(f"{processed_absolute}/races") \
.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date") 

# COMMAND ----------

# Reading 'processed/constructors' delta table.
constructorsDf = spark.read.format("delta").load(f"{processed_absolute}/constructors") \
.withColumnRenamed("name", "team") 

# COMMAND ----------

# Reading 'processed/results' delta table.
resultsDf = spark.read.format("delta").load(f"{processed_absolute}/results") \
.filter(f"file_date = '{v_file_date}'") \
.withColumnRenamed("time", "race_time") \
.withColumnRenamed("race_id", "result_race_id") \
.withColumnRenamed("file_date", "result_file_date") 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Race Results Join Transformation

# COMMAND ----------

# Join races with circuits
race_circuits_df = racesDf.join(circuitsDf, racesDf.circuit_id == circuitsDf.circuit_id, "inner") \
.select(racesDf.race_id, racesDf.race_year, racesDf.race_name, racesDf.race_date, circuitsDf.circuit_location)

# COMMAND ----------

race_results_df = resultsDf.join(race_circuits_df, resultsDf.result_race_id == race_circuits_df.race_id) \
                            .join(driversDf, resultsDf.driver_id == driversDf.driver_id) \
                            .join(constructorsDf, resultsDf.constructor_id == constructorsDf.constructor_id)

# COMMAND ----------

final_df = race_results_df.select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality",
                                 "team", "grid", "fastest_lap", "race_time", "points", "position", "result_file_date") \
                          .withColumn("created_date", current_timestamp()) \
                          .withColumnRenamed("result_file_date", "file_date") 

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_presentation', 'race_results', presentation_absolute, merge_condition, 'race_id')

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results;

# COMMAND ----------


