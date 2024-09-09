# Databricks notebook source
# MAGIC %md
# MAGIC ### Transforming *"constructors.json"*

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

# Reading 'constructors.json' using predefined schema.
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"
constructors_df = spark.read.schema(constructors_schema).json('{}/{}/constructors.json'.format(raw_absolute, v_file_date))

# COMMAND ----------

constructors_processed_df = constructors_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp())\
                                             .withColumn("file_date", lit(v_file_date))\
                                             .drop("url")    

# COMMAND ----------

#constructors_processed_df.write.mode("overwrite").parquet('{}/constructors'.format(processed_absolute))
constructors_processed_df.write.mode("overwrite").format('delta').saveAsTable('f1_processed.constructors')

# COMMAND ----------

dbutils.notebook.exit("Success")
