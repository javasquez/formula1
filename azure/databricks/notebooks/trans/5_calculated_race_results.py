# Databricks notebook source
# MAGIC %sql
# MAGIC use f1_processed

# COMMAND ----------

dbutils.widgets.text('p_file_date', '')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

spark.sql(f"""

CREATE TABLE IF NOT EXISTS f1_presentation.calculated_results (    race_year INT,   team_name STRING, driver_id INT,  driver_name STRING,race_id INT,    position INT,   points FLOAT,   calculated_points INT, createdDate TIMESTAMP, updatedDate TIMESTAMP) USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW race_results_updated
AS
SELECT races.race_year,
       constructors.name AS team_name,
       drivers.driver_id,
       results.race_id,
       drivers.name AS driver_name,
       results.position,
       results.points,
       11 - results.position AS calculated_points
  FROM f1_processed.results 
  JOIN f1_processed.drivers ON (results.driver_id = drivers.driver_id)
  JOIN f1_processed.constructors ON (results.constructorId = constructors.constructor_id)
  JOIN f1_processed.races ON (results.race_id = races.race_id)
 WHERE results.position <= 10
 AND f1_processed.results.file_date = '{v_file_date}'

""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from race_results_updated

# COMMAND ----------

spark.sql(f"""

MERGE INTO f1_presentation.calculated_results a
USING race_results_updated b
on a.driver_id = b.driver_id and a.race_id = b.race_id
WHEN MATCHED THEN
  UPDATE set a.position = b.position,
  a.points = b.points,
  a.calculated_points = b.points,
  a.updatedDate =  CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
  insert (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, createdDate) 
  values (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, CURRENT_TIMESTAMP)   
""")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(1) from f1_presentation.calculated_results

# COMMAND ----------

