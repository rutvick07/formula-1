# Databricks notebook source
# MAGIC %md
# MAGIC ### Transforming *"results.json"*

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

# Reading 'results.json' file using predefined schema.
results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                            StructField("raceId", IntegerType(), True),
                            StructField("driverId", IntegerType(), True),
                            StructField("constructorId", IntegerType(), True),
                            StructField("number", IntegerType(), True),
                            StructField("grid", IntegerType(), True),
                            StructField("position", IntegerType(), True),
                            StructField("positionText", StringType(), True),
                            StructField("positionOrder", IntegerType(), True),
                            StructField("points", FloatType(), True),
                            StructField("laps", IntegerType(), True),
                            StructField("time", StringType(), True),
                            StructField("milliseconds", IntegerType(), True),
                            StructField("fastestLap", IntegerType(), True),
                            StructField("rank", IntegerType(), True),
                            StructField("fastestLapTime", StringType(), True),
                            StructField("fastestLapSpeed", FloatType(), True),
                            StructField("statusId", StringType(), True)])
results_df = spark.read.json("{}/{}/results.json".format(raw_absolute, v_file_date), schema=results_schema)
results_df.printSchema()

# COMMAND ----------

# Transforming column names to snake case and dropping 'statusId' column.
results_processed_df = results_df.withColumn('ingestion_date', current_timestamp()).withColumn("file_date", lit(v_file_date)).withColumnsRenamed({
    "resultId": "result_id","driverId": "driver_id","constructorId": "constructor_id","positionText": "position_text","positionOrder": "position_order","fastestLap": "fastest_lap","fastestLapTime": "fastest_lap_time","fastestLapSpeed": "fastest_lap_speed","raceId": "race_id"
}).drop('statusId')

# COMMAND ----------

results_processed_df.count()

# COMMAND ----------

# Drop the duplicate records based on race_id and driver_id.
final_df = results_processed_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'results', processed_absolute, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, driver_id, COUNT(1) 
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id, driver_id
# MAGIC HAVING COUNT(1) > 1
# MAGIC ORDER BY race_id, driver_id DESC;

# COMMAND ----------

dbutils.notebook.exit("Success")
