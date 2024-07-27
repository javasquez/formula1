# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create database if not exists f1_demo
# MAGIC location '/mnt/formula1dljavi/demo'

# COMMAND ----------

results_df = spark.read.option('inferSchema', True).json('/mnt/formula1dljavi/raw/2021-03-28/results.json')

# COMMAND ----------

results_df.write.format('parquet').mode('overwrite').saveAsTable('f1_demo.results_managed_parquet')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table f1_demo.results_managed_parquet

# COMMAND ----------

results_df.write.format('delta').mode('overwrite').saveAsTable('f1_demo.results_managed')

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC show databases;

# COMMAND ----------

# MAGIC %sql 
# MAGIC use f1_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

results_df.write.format('delta').mode('overwrite').save('/mnt/formula1dljavi/demo/results_external')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table f1_demo.results_external
# MAGIC using DELTA
# MAGIC location "/mnt/formula1dljavi/demo/results_external"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from f1_demo.results_external

# COMMAND ----------

results_external_df = spark.read.format('delta').option('inferSchema', True).load('/mnt/formula1dljavi/demo/results_external')

# COMMAND ----------

display (results_external_df)

# COMMAND ----------

results_df.write.format('delta').mode('overwrite').partitionBy('constructorId').saveAsTable('f1_demo.results_partitioned')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from f1_demo.results_partitioned

# COMMAND ----------

# MAGIC  %sql
# MAGIC  
# MAGIC  show PARTITIONS f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC update f1_demo.results_partitioned
# MAGIC set points = 11-position
# MAGIC where position <= 10
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from f1_demo.results_partitioned

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *




# COMMAND ----------

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1dljavi/demo/results_managed')

# COMMAND ----------

deltaTable.update('position <= 10 ', {'points' : '21 - position '})

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC delete from f1_demo.results_managed where position >10

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1dljavi/demo/results_managed')

# COMMAND ----------

deltaTable.delete('points = 0' )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

driver_day1_df= spark.read.option('inferSchema', True).json('/mnt/formula1dljavi/raw/2021-03-28/drivers.json')\
.filter("driverId <= 10")\
.select ('driverId', 'dob', "name.forename", "name.surname")

# COMMAND ----------

driver_day1_df.createOrReplaceTempView('drivers_day1')

# COMMAND ----------

display(driver_day1_df)

# COMMAND ----------

driver_day2_df= spark.read.option('inferSchema', True).json('/mnt/formula1dljavi/raw/2021-03-28/drivers.json')\
.filter("driverId  between 6 and 15")\
.select ('driverId', 'dob', upper("name.forename").alias('forename'), upper("name.surname").alias('surname'))

# COMMAND ----------

driver_day2_df.createOrReplaceTempView('drivers_day2')

# COMMAND ----------

display (driver_day2_df)

# COMMAND ----------

driver_day3_df= spark.read.option('inferSchema', True).json('/mnt/formula1dljavi/raw/2021-03-28/drivers.json')\
.filter("driverId  between 1 and 5  or driverId between 16 and 20")\
.select ('driverId', 'dob', upper("name.forename").alias('forename'), upper("name.surname").alias('surname'))

# COMMAND ----------

display(driver_day3_df)

# COMMAND ----------

driver_day3_df.createOrReplaceTempView('drivers_day3')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge(
# MAGIC driverId INT, 
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO f1_demo.drivers_merge a
# MAGIC USING drivers_day1 b
# MAGIC on a.driverId = b.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE set a.dob = b.dob,
# MAGIC   a.forename = b.forename,
# MAGIC   a.surname = b.surname,
# MAGIC   a.updatedDate =  CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   insert (driverId, dob, forename, surname, createdDate) values (b.driverId, b.dob, b.forename, b.surname, CURRENT_TIMESTAMP)  

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM  f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO f1_demo.drivers_merge a
# MAGIC USING drivers_day2 b
# MAGIC on a.driverId = b.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE set a.dob = b.dob,
# MAGIC   a.forename = b.forename,
# MAGIC   a.surname = b.surname,
# MAGIC   a.updatedDate =  CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   insert (driverId, dob, forename, surname, createdDate) values (b.driverId, b.dob, b.forename, b.surname, CURRENT_TIMESTAMP)  

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from drivers_day2

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

from delta.tables import *

deltaTable  = DeltaTable.forPath(spark, '/mnt/formula1dljavi/demo/drivers_merge')

 
deltaTable.alias('a') \
  .merge(
    driver_day3_df.alias('b'),
    'a.driverId = b.driverId'
  ) \
  .whenMatchedUpdate(set =
    {
      "dob": "b.dob",
      "forename": "b.forename",
      "surname": "b.surname",
      "a.updatedDate": "current_timestamp()"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "b.driverId",
      "dob": "b.dob",
      "forename": "b.forename",
      "surname": "b.surname",
      "createdDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

spark_df = spark.read.option('inferSchema', True).load('/mnt/formula1dljavi/demo/drivers_merge')

# COMMAND ----------

display(spark_df)

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC desc history f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from  f1_demo.drivers_merge version as of 1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from  f1_demo.drivers_merge timestamp as of '2023-01-08T19:58:20.000+0000'

# COMMAND ----------

spark_df = spark.read.option('timestampAsOf', '2023-01-08T19:59:38.000+0000').load('/mnt/formula1dljavi/demo/drivers_merge')

# COMMAND ----------

display(spark_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC delete from f1_demo.drivers_merge where driverId = 1

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_demo.drivers_merge VERSION AS OF 3

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO f1_demo.drivers_merge a
# MAGIC USING f1_demo.drivers_merge VERSION AS OF 3 b
# MAGIC on a.driverId = b.driverId
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn(
# MAGIC driverId INT, 
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA 

# COMMAND ----------

# MAGIC %sql
# MAGIC desc HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO  f1_demo.drivers_txn
# MAGIC SELECT * FROM  f1_demo.drivers_merge where driverId = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta(
# MAGIC driverId INT, 
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET 

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC insert into f1_demo.drivers_convert_to_delta
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta

# COMMAND ----------

df = spark.table('f1_demo.drivers_convert_to_delta')

# COMMAND ----------

df.write.format('parquet').save('/mnt/formula1dljavi/demo/drivers_convert_to_delta_new')

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/mnt/formula1dljavi/demo/drivers_convert_to_delta_new`

# COMMAND ----------

