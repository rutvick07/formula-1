# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/config_variables"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hive_metastore

# COMMAND ----------

presentation_absolute

# COMMAND ----------

v_file_date

# COMMAND ----------

# Reading the transformed race results data to 'race_resultsdf'
race_resultsdf = spark.read.format('delta').load('{}/race_results'.format(presentation_absolute)).filter("file_date=='{}'".format(v_file_date))

# COMMAND ----------

race_year_list = df_column_to_list(race_resultsdf, 'race_year')

# COMMAND ----------

race_year_list

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_absolute}/race_results") \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

groupedDf = race_results_df.groupBy('race_year','driver_name','driver_nationality','team').agg(sum('points').alias('total_points'), count(when(col('position') == 1, 1)).alias('wins'))
groupedDf.show()

# COMMAND ----------

# Window Functions
from pyspark.sql import Window
window_spec = Window.partitionBy('race_year').orderBy(desc('total_points'),desc('wins'))
driver_standings = groupedDf.withColumn('rank',rank().over(window_spec))

# COMMAND ----------

driver_standings.filter('race_year == 2020').show()

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
merge_delta_data(driver_standings, 'f1_presentation', 'driver_standings', presentation_absolute, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year, COUNT(1)
# MAGIC   FROM f1_presentation.driver_standings
# MAGIC  GROUP BY race_year
# MAGIC  ORDER BY race_year DESC;

# COMMAND ----------


