# Databricks notebook source
# MAGIC %fs
# MAGIC ls /mnt/formula1dljavi/raw/
# MAGIC

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')


# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType


# COMMAND ----------

lamptime_schema = StructType (fields=[StructField('raceId', IntegerType(), True),
                                  StructField('driverId', IntegerType(), True),
                                  StructField('lap', IntegerType(), True),
                                  StructField('position', IntegerType(), True),
                                  StructField('time', StringType(), True),
                                  StructField('milliseconds', StringType(), True),
                                  

])

# COMMAND ----------

df_laptimes = spark.read.schema(lamptime_schema).csv(f'{raw_folder_path}/{v_file_date}/lap_times/lap_times*')
display(df_laptimes)

# COMMAND ----------

df_laptimes.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat, col

# COMMAND ----------

df_laptimes_transformed = df_laptimes.withColumnRenamed('raceId', 'race_id').withColumnRenamed('driverId', 'driver_id')

df_laptimes_transformed= add_ingestion_date(df_laptimes_transformed).withColumn('data_source', lit(v_data_source)).withColumn("file_date", lit(v_file_date))

display(df_laptimes_transformed)


# COMMAND ----------

display(df_laptimes_transformed)

# COMMAND ----------

merge_condition = 'a.driver_id = b.driver_id and a.race_id = b.race_id and a.lap = b.lap'
merge_delta_data(df_laptimes_transformed, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct file_date from f1_processed.lap_times

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table f1_processed.pit_stops

# COMMAND ----------

