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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType


# COMMAND ----------

circuits_schema = StructType(fields=[StructField('circuitId', IntegerType(), False),
                                      StructField('circuitRef', StringType(),True),
                                      StructField('name', StringType(),True),
                                      StructField('location', StringType(),True),
                                      StructField('country', StringType(),True),
                                      StructField('lat', DoubleType(),True),
                                      StructField('lng', DoubleType(),True),
                                      StructField('alt', IntegerType(),True),
                                      StructField('url', IntegerType(),True)         
                                                  
                                                
                                                ] 

)

# COMMAND ----------

df_circuits = spark.read.options( header = True).schema(circuits_schema).csv(f'{raw_folder_path}/{v_file_date}/circuits.csv')
df_circuits.show()

# COMMAND ----------

df_circuits.printSchema()

# COMMAND ----------

circuits_selected_df = df_circuits.select('circuitId','circuitRef','name', 'location', 'country','lat','lng','alt' )

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

circuits_renamed = circuits_selected_df.withColumnRenamed('circuitId', 'circuit_id').withColumnRenamed('circuitRef', 'circuit_ref')\
.withColumnRenamed('lat', 'latitude')\
.withColumnRenamed('lng', 'longitude')\
.withColumnRenamed('alt', 'altitude')


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# MAGIC
# MAGIC %run "../includes/common_functions"
# MAGIC

# COMMAND ----------

circuits_renamed = add_ingestion_date(circuits_renamed).withColumn('data_source', lit(v_data_source)).withColumn('file_date', lit(v_file_date))

# COMMAND ----------

circuits_renamed.write.mode('overwrite').format('delta').saveAsTable('f1_processed.circuits')

# COMMAND ----------

dbutils.notebook.exit('Success')