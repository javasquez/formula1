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

qualifying_schema = StructType (fields=[StructField('qualifyId', IntegerType(), True),
                                        StructField('raceId', IntegerType(), True),
                                  StructField('driverId', IntegerType(), True),
                                        StructField('constructorId', IntegerType(), True),
                                  StructField('number', IntegerType(), True),
                                  StructField('position', IntegerType(), True),
                                  StructField('q1', StringType(), True),
                                  StructField('q2', StringType(), True),
                                        StructField('q3', StringType(), True)
                                  

])

# COMMAND ----------

df_qualifying = spark.read.schema(qualifying_schema).option('multiLine',True).json(f'{raw_folder_path}/{v_file_date}/qualifying/')
display(df_qualifying)

# COMMAND ----------

df_qualifying.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat, col

# COMMAND ----------

df_qualifying_transformed = df_qualifying.withColumnRenamed('qualifyId', 'qualify_id').withColumnRenamed('raceId', 'race_id').withColumnRenamed('driverId', 'driver_id').withColumnRenamed('constructorId', 'constructor_id')

df_qualifying_transformed= add_ingestion_date(df_qualifying_transformed).withColumn('data_source', lit(v_data_source)).withColumn("file_date", lit(v_file_date))


display(df_qualifying_transformed)


# COMMAND ----------

display(df_qualifying_transformed)

# COMMAND ----------

merge_condition = 'a.qualify_id = b.qualify_id and a.race_id = b.race_id'
merge_delta_data(df_qualifying_transformed, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct file_date from f1_processed.qualifying

# COMMAND ----------

