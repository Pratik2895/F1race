# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore the capabilities of the dbutils.secrets utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'adlsf1scope')

# COMMAND ----------

dbutils.secrets.get(scope = 'adlsf1scope', key = 'adlsf1accountaccesskey')

# COMMAND ----------

