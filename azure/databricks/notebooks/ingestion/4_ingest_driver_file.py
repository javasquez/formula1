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

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType


# COMMAND ----------

name_schema = StructType (fields=[ StructField('forename', StringType(), True),
                                   StructField('surname', StringType(), True)

])

# COMMAND ----------

driver_schema = StructType (fields=[ StructField('driverId', IntegerType(), True),
                                   StructField('driverRef', StringType(), True),
                                     StructField('number', IntegerType(), True),
                                     StructField('code', StringType(), True),
                                     StructField('name', name_schema, True),
                                     StructField('dob', DateType(), True),
                                     StructField('nationality', StringType(), True),
                                     StructField('url', StringType(), True)

])

# COMMAND ----------

df_drivers = spark.read.schema(driver_schema).json(f'{raw_folder_path}/{v_file_date}/drivers.json')
df_drivers.show()

# COMMAND ----------

df_drivers.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat, col

# COMMAND ----------

df_drivers_transformed = df_drivers.withColumnRenamed('driverId', 'driver_id').withColumnRenamed('driverRef', 'driver_ref')

df_drivers_transformed= add_ingestion_date(df_drivers_transformed)


df_drivers_transformed = df_drivers_transformed.withColumn('name', concat(col('name').forename,lit(' '), col('name').surname)).drop('url').withColumn('data_source', lit(v_data_source)).withColumn('file_date', lit(v_file_date))



df_drivers_transformed.show()

# COMMAND ----------

display(df_drivers_transformed)

# COMMAND ----------

df_drivers_transformed.write.mode('overwrite').format('delta').saveAsTable('f1_processed.drivers')

# COMMAND ----------

dbutils.notebook.exit('Success')