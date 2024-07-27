# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_result_df = spark.read.parquet(f'{presentation_folder_path}/races_results')

# COMMAND ----------

races_result_df.createOrReplaceTempView('v_race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from v_race_results where race_year = 2020
# MAGIC

# COMMAND ----------

v_race_year = 2020
dfSqled = spark.sql (f"select  * from v_race_results where race_year = {v_race_year}")
display(dfSqled)

# COMMAND ----------

races_result_df.createOrReplaceGlobalTempView('gv_race_results')

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC show tables in global_temp;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from global_temp.gv_race_results;

# COMMAND ----------

