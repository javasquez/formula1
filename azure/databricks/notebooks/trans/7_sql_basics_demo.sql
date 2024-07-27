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

