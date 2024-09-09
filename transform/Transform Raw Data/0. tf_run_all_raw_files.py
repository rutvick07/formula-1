# Databricks notebook source
v_result = dbutils.notebook.run("1. tf_circuits_csv", 0, {"p_file_date": "2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2. tf_constructors_json", 0, {"p_file_date": "2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3. tf_races_csv", 0, {"p_file_date": "2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4. tf_drivers_json", 0, {"p_file_date": "2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5. tf_results_json", 0, {"p_file_date": "2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6. tf_pit_stops_json", 0, {"p_file_date": "2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7. tf_lap_times_csv", 0, {"p_file_date": "2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8. tf_qualifying_json", 0, {"p_file_date": "2021-03-21"})

# COMMAND ----------

v_result
