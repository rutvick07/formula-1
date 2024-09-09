# Databricks notebook source
# MAGIC %md
# MAGIC ### Transforming *"pitstops.json"*

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

pitstops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])
pitstops_df = spark.read.json('{}/{}/pit_stops.json'.format(raw_absolute, v_file_date), schema=pitstops_schema, multiLine=True)
display(pitstops_df)                                    

# COMMAND ----------

pitstops_processed_df = pitstops_df.withColumnRenamed("driverId", "driver_id") \
                                .withColumnRenamed("raceId", "race_id") \
                                .withColumn("ingestion_date", current_timestamp())\
                                .withColumn("file_date", lit(v_file_date))    
# pitstops_processed_df.write.mode("overwrite").parquet("{}/pitstops".format(processed_absolute))
#pitstops_processed_df.write.mode("overwrite").format('parquet').saveAsTable('f1_processed.pitstops')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(pitstops_processed_df, 'f1_processed', 'pit_stops', processed_absolute, merge_condition, 'race_id')

# COMMAND ----------

+dbutils.notebook.exit("Success")
