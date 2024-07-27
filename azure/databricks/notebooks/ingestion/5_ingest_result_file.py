# Databricks notebook source
# MAGIC   %fs
# MAGIC ls /mnt/formula1dljavi/raw/
# MAGIC

# COMMAND ----------



# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')


# COMMAND ----------

dbutils.widgets.text('p_file_date', '')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType


# COMMAND ----------

result_schema = StructType (fields=[ StructField('resultId', IntegerType(), True),
                                   StructField('raceId', IntegerType(), True),
                                  StructField('driverId', IntegerType(), True),
                                  StructField('constructorId', IntegerType(), True),
                                  StructField('number', IntegerType(), True),
                                  StructField('grid', IntegerType(), True),
                                  StructField('position', IntegerType(), True),
                                  StructField('positionText', StringType(), True),
                                  StructField('positionOrder', IntegerType(), True),
                                  StructField('points', FloatType(), True),
                                  StructField('laps', IntegerType(), True),
                                  StructField('time', StringType(), True),
                                  StructField('milliseconds', IntegerType(), True),
                                  StructField('fastestLap', IntegerType(), True),
                                  StructField('rank', IntegerType(), True),
                                  StructField('fastestLapTime', StringType(), True),
                                  StructField('fastestLapSpeed', StringType(), True),
                                  StructField('statusId', IntegerType(), True)
                                  

])

# COMMAND ----------

df_results = spark.read.schema(result_schema).json(f'{raw_folder_path}/{v_file_date}/results.json')
display(df_results)

# COMMAND ----------

df_results.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat, col

# COMMAND ----------

df_results_transformed = df_results.withColumnRenamed('resultId', 'result_id').withColumnRenamed('raceId', 'race_id').withColumnRenamed('driverId', 'driver_id').withColumnRenamed('contructorId', 'contructor_id').withColumnRenamed('positionText', 'position_text').withColumnRenamed('positionOrder', 'position_order').withColumnRenamed('fastestLap', 'fastest_lap').withColumnRenamed('fastestLapTime', 'fastest_lap_time').withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed').drop('statusId')

df_results_transformed = add_ingestion_date(df_results_transformed).withColumn('data_source', lit(v_data_source)).withColumn('file_date', lit(v_file_date))


df_results_reduped= df_results_transformed.drop_duplicates(['race_id', 'driver_id'])


# COMMAND ----------

display(df_results_transformed)

# COMMAND ----------


#for x in df_results_transformed.select('race_id').distinct().collect():
#    if (spark._jsparkSession.catalog().tableExists('f1_processed.results')):
#        spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id =  {x.race_id})")

# COMMAND ----------

merge_condition = 'a.result_id = b.result_id and a.race_id = b.race_id'
merge_delta_data(df_results_reduped, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select distinct file_date from f1_processed.results

# COMMAND ----------



# COMMAND ----------

