# Databricks notebook source
# MAGIC %md
# MAGIC ### Transforming *"circuits.csv"*

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

# Reading 'circuits.csv' file using predefined schema.
circuits_schema = StructType([
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("long", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True),
])
circuits_df = spark.read.csv("{}/{}/circuits.csv".format(raw_absolute, v_file_date), header=True, schema=circuits_schema)
circuits_df.printSchema()

# COMMAND ----------

# Renaming ['circuitId', 'circuitRef', 'lat', 'long'] columns, dropping 'url' column and adding 'ingestion_date' column.
circuits_processed_df = circuits_df.drop('url') \
    .withColumn('ingestion_date', current_timestamp()) \
    .withColumn("file_date", lit(v_file_date)) \
    .withColumnsRenamed({'circuitId': 'circuit_id','circuitRef': 'circuit_ref','lat': 'latitude','long': 'longitude'})    

# COMMAND ----------

# Saving the processed data to '{prcssed_absolute}/processed/circuits' directory.
#circuits_processed_df.write.mode('overwrite').parquet('{}/circuits'.format(processed_absolute))
circuits_processed_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.circuits')

# COMMAND ----------

dbutils.notebook.exit("Success")
