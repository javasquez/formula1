# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------


df = spark.read.parquet(f'{presentation_folder_path}/races_results')
df.show()

# COMMAND ----------

df_filtered = df.filter('race_year = 2020')
df_filtered.count()

# COMMAND ----------

from pyspark.sql.functions import countDistinct, count, avg, sum

# COMMAND ----------

df_filtered.select(countDistinct('race_name')).show()

# COMMAND ----------

df_filtered.groupBy('race_name').sum('points').show()

# COMMAND ----------

df_filtered.groupBy('driver_name').agg(sum('points'), count('race_name')).sort(df_filtered.driver_name.desc()).show()

# COMMAND ----------

df_filtered_w = df.filter('race_year = 2020 or race_year = 2019')

# COMMAND ----------

df_filtered_w.count()

# COMMAND ----------

groupedW = df_filtered_w.groupBy('race_year', 'driver_name').agg(sum('points').alias('total_points'), count('race_name').alias('number_of_races'))


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

driverRank = Window.partitionBy('race_year').orderBy(desc('total_points'))

# COMMAND ----------

groupedW.withColumn('rank', rank().over(driverRank)).show(200)

# COMMAND ----------

df.show()

# COMMAND ----------

