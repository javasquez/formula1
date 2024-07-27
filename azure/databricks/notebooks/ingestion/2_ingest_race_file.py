# Databricks notebook source
# MAGIC %fs
# MAGIC ls /mnt/formula1dljavi/raw/
# MAGIC

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')


# COMMAND ----------

dbutils.widgets.text('p_file_date', '')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType


# COMMAND ----------

races_schema = StructType(fields=[StructField('raceId', IntegerType(), False),
                                      StructField('year', IntegerType(),True),
                                      StructField('round', IntegerType(),True),
                                      StructField('circuitId', IntegerType(),True),
                                      StructField('name', StringType(),True),
                                      StructField('date', StringType(),True),
                                      StructField('time', StringType(),True),
                                      StructField('url', StringType(),True)       
                                                  
                                                
                                                ] 

)

# COMMAND ----------

df_races = spark.read.options( header = True).schema(races_schema).csv(f'{raw_folder_path}/{v_file_date}/races.csv')
df_races.show()

# COMMAND ----------

df_races.printSchema()

# COMMAND ----------

races_selected_df = df_races.drop('url')

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

races_renamed = races_selected_df.withColumnRenamed('raceId', 'race_id').withColumnRenamed('year', 'race_year')\
.withColumnRenamed('circuitId', 'circuit_id')


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat, col


# COMMAND ----------

races_final = races_renamed.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')) ,'yyyy-MM-dd HH:mm:ss'  )).drop('date').drop('time')

# COMMAND ----------


 
races_final = add_ingestion_date(races_final).withColumn('data_source', lit(v_data_source)).withColumn('file_date', lit(v_file_date))

races_final.printSchema()

# COMMAND ----------

races_final.write.mode('overwrite').format('delta').saveAsTable('f1_processed.races')

# COMMAND ----------

dbutils.notebook.exit('Success')