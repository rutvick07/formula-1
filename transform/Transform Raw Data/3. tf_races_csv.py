# Databricks notebook source
# MAGIC %md
# MAGIC ### Transforming *"races.csv"*

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

# Reading 'races.csv' file using predefined schema.
races_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True),
])
races_df = spark.read.csv("{}/{}/races.csv".format(raw_absolute, v_file_date), header=True, schema=races_schema)
races_df.printSchema()

# COMMAND ----------

# Adding columns ['race_timestamp', 'ingestion_date'] and dropping columns ['url', 'date', 'time'] from 'races_df'.
races_processed_df = races_df.withColumns({'race_timestamp': to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'), 'ingestion_date': current_timestamp()}).withColumnsRenamed({'raceId':'race_id','year':'race_year','circuitId':'circuit_id'}).withColumn("file_date", lit(v_file_date)).drop('url','date','time')

# COMMAND ----------

# Writing processed data to '{processed_absolute}/processed/races' directory.
# races_processed_df.write.mode('overwrite').partitionBy('race_year').parquet('{}/races'.format(processed_absolute))
races_processed_df.write.mode('overwrite').partitionBy('race_year').format('delta').saveAsTable('f1_processed.races')

# COMMAND ----------

dbutils.notebook.exit("Success")
