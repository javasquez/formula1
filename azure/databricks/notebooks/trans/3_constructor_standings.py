# Databricks notebook source
dbutils.widgets.text('p_file_date', '')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------


results_df = spark.read.format('delta').load(f'{presentation_folder_path}/races_results').filter(f"file_date = '{v_file_date}'") 


# COMMAND ----------

race_year_list = df_column_to_list(results_df, 'race_year')


# COMMAND ----------

from pyspark.sql.functions import countDistinct, count, avg, sum ,when, col


# COMMAND ----------


df = spark.read.format('delta').load(f'{presentation_folder_path}/races_results').filter(col('race_year').isin(race_year_list))


# COMMAND ----------

groupedW = df.groupBy('race_year','team').agg(sum('points').alias('total_points'), count( when(df.position == 1, True)).alias('wins') ) 




# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

driverRank = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))

# COMMAND ----------

df_ranked = groupedW.withColumn('rank', rank().over(driverRank))

# COMMAND ----------


merge_condition = 'a.team = b.team and a.race_year = b.race_year'
merge_delta_data(df_ranked, 'f1_presentation', 'constructor_standings', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *
# MAGIC from f1_presentation.constructor_standings
# MAGIC where race_year = 2021

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select race_year, count(1)
# MAGIC from f1_presentation.constructor_standings
# MAGIC group by race_year
# MAGIC order by race_year desc

# COMMAND ----------

