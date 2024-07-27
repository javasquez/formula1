-- Databricks notebook source
show databases;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

use f1_processed

-- COMMAND ----------

select number 
from f1_processed.drivers where nationality = 'British';

-- COMMAND ----------

select split(name, ' ')[0] name,  split(name, ' ')[1] name
from drivers

-- COMMAND ----------

select dob , date_format(dob, 'yyyy')
from drivers

-- COMMAND ----------

select * from drivers

-- COMMAND ----------

show databases

-- COMMAND ----------

use f1_presentation

-- COMMAND ----------

show tables

-- COMMAND ----------

select * from races_results

-- COMMAND ----------

select race_year,  team , points,  dense_rank() over ( PARTITION BY race_year order by  points desc) as team_rank
from races_results

-- COMMAND ----------

use f1_processed

-- COMMAND ----------

select nationality,name,   dob , rank () over ( Partition by nationality order  by dob desc ) as age_rank 
from drivers
order by nationality 

-- COMMAND ----------


 order 