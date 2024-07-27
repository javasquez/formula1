# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races').select('circuit_id','race_id', 'race_year', 'name','race_timestamp').withColumnRenamed('name', 'race_name').withColumnRenamed('race_timestamp', 'race_date').filter('race_year = 2020')

races_df = races_df.filter(races_df.race_name == 'Abu Dhabi Grand Prix')


races_df.show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------


circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits').withColumnRenamed('name','circuit_name').withColumnRenamed('location','circuit_location')

circuits_df = circuits_df.select(circuits_df.circuit_id, circuits_df.circuit_location)
circuits_df.show()



# COMMAND ----------

results_df = spark.read.parquet(f'{processed_folder_path}/results')


# COMMAND ----------

results_df= results_df.select (results_df.race_id, results_df.driver_id, 'constructorId', results_df.grid, results_df.fastest_lap_time, results_df.time,results_df.points ).withColumnRenamed('time', 'race_time')

display(results_df)

# COMMAND ----------

cosntructors_df = spark.read.parquet(f'{processed_folder_path}/constructors').select('constructor_id','name' ).withColumnRenamed('name', 'team')
display(cosntructors_df)

# COMMAND ----------

drivers_df = spark.read.parquet(f'{processed_folder_path}/drivers').select('driver_id', 'name', 'number', 'nationality').withColumnRenamed('name', 'driver_name').withColumnRenamed('number', 'driver_number').withColumnRenamed('nationality', 'driver_nationality')
display(drivers_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

report_df = results_df.join(races_df, results_df.race_id ==races_df.race_id, 'inner').\
                       join(drivers_df, results_df.driver_id ==drivers_df.driver_id, 'inner').\
                        join(cosntructors_df, results_df.constructorId ==cosntructors_df.constructor_id, 'inner').\
                        join(circuits_df, races_df.circuit_id ==circuits_df.circuit_id, 'inner').\
                        select (races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location,drivers_df.driver_name,drivers_df.driver_number, drivers_df.driver_nationality,cosntructors_df.team,
                          results_df.grid, results_df.fastest_lap_time, results_df.race_time, results_df.points).withColumn('created_dame', current_timestamp()).sort(results_df.points.desc())

display(report_df)



# COMMAND ----------

report_df.write.mode('overwrite').parquet(f'{processed_folder_path}/races_results')

# COMMAND ----------

