# Databricks notebook source


storage_account_name= "formula1dljavi"

# COMMAND ----------

dbutils.secrets.listScopes()
dbutils.secrets.list('formula1--scope')

client_id=dbutils.secrets.get(scope ='formula1--scope',key = 'formula1-clientid-app' )
tenant_id = dbutils.secrets.get(scope ='formula1--scope',key = 'formula1-tenantid-app' )
client_secret=dbutils.secrets.get(scope ='formula1--scope',key = 'formula1-client-app' )



# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

container_name = "raw"
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)


# COMMAND ----------

container_name = "presentation"
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)


# COMMAND ----------


dbutils.fs.ls("/mnt/formula1dljavi/raw")



# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

container_name = "processed"
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)


# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

container_name = "demo"
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

