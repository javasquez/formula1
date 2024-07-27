# Databricks notebook source
v_result= dbutils.notebook.run('1_ingest_circuit_file',0, {'p_data_source': 'Eargast API' ,'p_file_date' : '2021-04-18' })

# COMMAND ----------

v_result


# COMMAND ----------

v_result= dbutils.notebook.run('2_ingest_race_file',0, {'p_data_source': 'Eargast API','p_file_date' : '2021-04-18'})

# COMMAND ----------

v_result

# COMMAND ----------

v_result= dbutils.notebook.run('3_ingest_constructor_file',0, {'p_data_source': 'Eargast API','p_file_date' : '2021-04-18'})

# COMMAND ----------

v_result

# COMMAND ----------

v_result= dbutils.notebook.run('4_ingest_driver_file',0, {'p_data_source': 'Eargast API','p_file_date' : '2021-04-18'})

# COMMAND ----------

v_result

# COMMAND ----------

v_result= dbutils.notebook.run('5_ingest_result_file',0, {'p_data_source': 'Eargast API','p_file_date' : '2021-04-18'})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run(
    "6_ingest_pitstop_file",
    0,
    {"p_data_source": "Eargast API", "p_file_date": "2021-04-18"},
)

# COMMAND ----------

v_result

# COMMAND ----------

v_result= dbutils.notebook.run('7_ingest_laptimes_file',0, {'p_data_source': 'Eargast API','p_file_date' : '2021-04-18'})

# COMMAND ----------

v_result

# COMMAND ----------

v_result= dbutils.notebook.run('8_ingest_qualifying_file',0, {'p_data_source': 'Eargast API','p_file_date' : '2021-04-18'})

# COMMAND ----------

v_result

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select distinct file_date from f1_processed.qualifying

# COMMAND ----------

