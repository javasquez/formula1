-- Databricks notebook source
CREATE DATABASE demo;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

 SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE DEMO;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

use demo;

-- COMMAND ----------

SHOW TABLES in default;

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC race_result_df = spark.read.parquet(f'{presentation_folder_path}/races_results')
-- MAGIC display(race_result_df)

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC race_result_df.write.format('parquet').saveAsTable('demo.race_results_python')

-- COMMAND ----------

select * from race_results_python

-- COMMAND ----------

DESC EXTENDED race_results_python

-- COMMAND ----------

CREATE TABLE race_results_sql AS 
SELECT * FROM demo.race_results_python where race_year = 2020

-- COMMAND ----------

drop demo.race_results_sql

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df.write.format('parquet').option ('path', f'{presentation_folder_path}/race_results_ext_py').saveAsTable('demo.race_results_python_ext_py')

-- COMMAND ----------

describe extended demo.race_results_python_ext_py

-- COMMAND ----------

create table demo.race_results_python_ext_sql(
race_year	int	,
race_name	string	,
race_date	timestamp,	
circuit_location	string	,
driver_name	string	,
driver_number	int	,
driver_nationality	string,	
team	string	,
grid	int	,
fastest_lap	int,	
race_time	string	,
points	float	,
position	int	,
created_dame	timestamp
) 
USING parquet
location "/mnt/formula1dljavi/presentation/race_results_ext_sql"

-- COMMAND ----------

insert into demo.race_results_python_ext_sql
select * from demo.race_results_python_ext_py where race_year= 2020

-- COMMAND ----------

drop table demo.race_results_python_ext_sql

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

create temp view v_race_results
as 
select * 
from demo.race_results_python where race_year = 2018

-- COMMAND ----------

create or replace view v_race_results
as 
select * 
from demo.race_results_python where race_year = 2018

-- COMMAND ----------

create or replace global temp view v_race_results
as 
select * 
from demo.race_results_python where race_year = 2019

-- COMMAND ----------

select * from demo.race_results_python

-- COMMAND ----------

select * from global_temp.v_race_results

-- COMMAND ----------

create or replace view demo.permanent_view_race_results
as 
select * 
from demo.race_results_python where race_year = 2017

-- COMMAND ----------

show tables

-- COMMAND ----------

