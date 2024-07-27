# Databricks notebook source
dbutils.widgets.text('p_file_date', '')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

race_results_list = spark.sql(f"SELECT DISTINCT race_year from f1_presentation.races_results where file_date = '{v_file_date}'").collect()




# COMMAND ----------

race_year_list = []

for x in race_results_list:
    race_year_list.append(x.race_year)



# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df = spark.read.format('delta').load(f'{presentation_folder_path}/races_results').filter(col('race_year').isin(race_year_list))


# COMMAND ----------

from pyspark.sql.functions import countDistinct, count, avg, sum ,when, col


# COMMAND ----------

groupedW = df.groupBy('race_year','driver_name', 'driver_nationality').agg(sum('points').alias('total_points'), count( when(df.position == 1, True)).alias('wins') ) 




# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

driverRank = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))

# COMMAND ----------

df_ranked = groupedW.withColumn('rank', rank().over(driverRank))

# COMMAND ----------


merge_condition = 'a.driver_name = b.driver_name and a.race_year = b.race_year'
merge_delta_data(df_ranked, 'f1_presentation', 'driver_standings', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC select race_year, count(1)
# MAGIC from f1_presentation.driver_standings
# MAGIC group by race_year
# MAGIC order by race_year desc

# COMMAND ----------

