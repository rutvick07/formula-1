# Databricks notebook source
# MAGIC %md
# MAGIC ### Transforming *"qualifying.json"*

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

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                    StructField("raceId", IntegerType(), True),
                    StructField("driverId", IntegerType(), True),
                    StructField("constructorId", IntegerType(), True),
                    StructField("number", IntegerType(), True),
                    StructField("position", IntegerType(), True),
                    StructField("q1", StringType(), True),
                    StructField("q2", StringType(), True),
                    StructField("q3", StringType(), True),])
qualifying_df = spark.read.json('{}/{}/qualifying'.format(raw_absolute, v_file_date), schema=qualifying_schema, multiLine=True)

# COMMAND ----------

qualifying_processed_df = qualifying_df.withColumnsRenamed({"qualifyId": "qualify_id","driverId": "driver_id","raceId": "race_id", "constructorId": "constructor_id"}).withColumn("ingestion_date", current_timestamp()).withColumn("file_date", lit(v_file_date))
# qualifying_processed_df.write.mode("overwrite").parquet("{}/qualifying".format(processed_absolute))
#qualifying_processed_df.write.mode("overwrite").format('parquet').saveAsTable('f1_processed.qualifying')

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(qualifying_processed_df, 'f1_processed', 'qualifying', processed_absolute, merge_condition, 'race_id')

# COMMAND ----------

+dbutils.notebook.exit("Success")
