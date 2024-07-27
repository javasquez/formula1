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

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

df_constructors = spark.read.schema(constructor_schema).json(f'{raw_folder_path}/{v_file_date}/constructors.json')
df_constructors.show()

# COMMAND ----------

df_constructors.printSchema()

# COMMAND ----------

constructors_selected_df = df_constructors.drop('url')

# COMMAND ----------

display(constructors_selected_df)

# COMMAND ----------

constructors_renamed = constructors_selected_df.withColumnRenamed('constructorId', 'constructor_id').withColumnRenamed('constructorRef', 'constructor_ref')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat, col


# COMMAND ----------



constructors_final = add_ingestion_date(constructors_renamed).withColumn('data_source', lit(v_data_source)).withColumn('file_date', lit(v_file_date))

constructors_final.printSchema()

# COMMAND ----------

constructors_final.write.mode('overwrite').format('delta').saveAsTable('f1_processed.constructors')

# COMMAND ----------

dbutils.notebook.exit('Success')