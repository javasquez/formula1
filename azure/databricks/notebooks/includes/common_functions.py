# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    return input_df.withColumn('ingestion_date', current_timestamp())
    

# COMMAND ----------


def generateQuery (input_df, partition_column):
    
    column_list = []
    for column in input_df.schema.names:
        if column != partition_column:
            column_list.append(column)
    column_list.append(partition_column)
    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------

def overwritePartition(input_df, db_name, table_name, partition_column):
    output_df = generateQuery(input_df , partition_column)
    spark.conf.set ('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    if (spark._jsparkSession.catalog().tableExists(f'{db_name}.{table_name}')):
        output_df.write.mode('overwrite').insertInto(f'{db_name}.{table_name}')
    else:
        output_df.write.mode('append').partitionBy(f'{partition_column}').format('parquet').saveAsTable(f'{db_name}.{table_name}')
     
    

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
    spark.conf.set ('spark.databricks.optimizer.dynamic', 'dynamic')
    from delta.tables import DeltaTable
    if (spark._jsparkSession.catalog().tableExists(f'{db_name}.{table_name}')):
        deltaTable  = DeltaTable.forPath(spark, f'{folder_path}/{table_name}')
        deltaTable.alias('a').merge(input_df.alias('b'),merge_condition)\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
    else:
        input_df.write.mode('append').partitionBy(f'{partition_column}').format('delta').saveAsTable(f'{db_name}.{table_name}')
     

# COMMAND ----------

def df_column_to_list(input_df, column_name):
    df_row_list = input_df.select(column_name) \
                        .distinct() \
                        .collect()
    column_value_list = [row[column_name] for row in df_row_list]
    return column_value_list

# COMMAND ----------

