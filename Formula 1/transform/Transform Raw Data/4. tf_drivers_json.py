# Databricks notebook source
# MAGIC %md
# MAGIC ### Transforming *"drivers.json"*

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

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
  
])

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])
drivers_df = spark.read.json('{}/{}/drivers.json'.format(raw_absolute,v_file_date),schema=drivers_schema)
drivers_df.printSchema()

# COMMAND ----------

drivers_processed_df = drivers_df.withColumnsRenamed({"driverRef": "driver_ref","driverId" : "driver_id"})\
            .withColumns({"ingestion_date": current_timestamp(), "name": concat(col("name.forename"), lit(" "), col("name.surname"))}) \
            .withColumn("file_date", lit(v_file_date))\
            .drop("url",drivers_df.name.forename,drivers_df.name.surname)

# COMMAND ----------

# drivers_processed_df.write.mode("overwrite").parquet("{}/drivers".format(processed_absolute))
drivers_processed_df.write.mode("overwrite").format('delta').saveAsTable('f1_processed.drivers')

# COMMAND ----------

dbutils.notebook.exit("Success")
