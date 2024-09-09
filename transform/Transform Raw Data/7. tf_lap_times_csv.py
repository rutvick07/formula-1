# Databricks notebook source
# MAGIC %md
# MAGIC ### Transforming *"lap_times.csv"*

# COMMAND ----------

# Import Libraries and run Config Files
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "/Workspace/Users/moradiya7045711197@gmail.com/Formula 1/includes/config_variables"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/moradiya7045711197@gmail.com/Formula 1/includes/common_functions"

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hive_metastore;

# COMMAND ----------

laptimes_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                    StructField("driverId", IntegerType(), True),
                    StructField("lap", IntegerType(), True),
                    StructField("position", IntegerType(), True),
                    StructField("time", StringType(), True),
                    StructField("milliseconds", IntegerType(), True)])
laptime_df = spark.read.csv('{}/{}/lap_times'.format(raw_absolute, v_file_date), schema=laptimes_schema)

# COMMAND ----------

laptime_processed_df = laptime_df.withColumnsRenamed({"driverId": "driver_id","raceId": "race_id"})\
                                 .withColumn("ingestion_date", current_timestamp()).withColumn("file_date", lit(v_file_date))
# laptime_processed_df.write.mode("overwrite").parquet("{}/laptimes".format(processed_absolute))
#laptime_processed_df.write.mode("overwrite").format('parquet').saveAsTable('f1_processed.laptimes')                                

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(laptime_processed_df, 'f1_processed', 'lap_times', processed_absolute, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")
