-- Databricks notebook source
create database if not exists  f1_processed
location "/mnt/formula1dljavi/processed"

-- COMMAND ----------

desc database f1_processed;

-- COMMAND ----------

create database if not exists  f1_presentation
location "/mnt/formula1dljavi/presentation"

-- COMMAND ----------

