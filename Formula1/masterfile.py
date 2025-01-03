# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC ###Access Azure Data Lake using access keys
# MAGIC

# COMMAND ----------

# DBTITLE 1,sample
spark.conf.set(
    "fs.azure.account.key.<storage-account>.dfs.core.windows.net",
    dbutils.secrets.get(scope="<scope>", key="<storage-account-access-key>"))

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.adlsf1.dfs.core.windows.net",
    "rdkhmt0MlRLjExZNTsc5HeShp2NwtndQjMPhL0vLFwRxm1aYtq7P0O+hk4IOV/Tu/s68HDbIwzdJ+AStf5mLEg==")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@adlsf1.dfs.core.windows.net/"))

# COMMAND ----------

spark.read.format("csv").option("header", "true").load("abfss://demo@adlsf1.dfs.core.windows.net/circuits.csv").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC

# COMMAND ----------

# DBTITLE 1,sample
spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.<storage-account>.dfs.core.windows.net", dbutils.secrets.get(scope="<scope>", key="<sas-token-key>"))

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.adlsf1.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.adlsf1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.adlsf1.dfs.core.windows.net", "sp=rl&st=2024-12-29T18:10:43Z&se=2025-01-06T02:10:43Z&spr=https&sv=2022-11-02&sr=c&sig=b9c5VAqEvhPCT1Zahs1zxIa7dncDDi9nSZ6vk8U25kk%3D")

# COMMAND ----------

spark.read.format("csv").option("header", "true").load("abfss://demo@adlsf1.dfs.core.windows.net/circuits.csv").display()

# COMMAND ----------



# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@adlsf1.dfs.core.windows.net/"))

# COMMAND ----------

# MAGIC %md
# MAGIC --https://adb-4117005333018026.6.azuredatabricks.net/?o=4117005333018026#secrets/createScope

# COMMAND ----------

# MAGIC %sql
# MAGIC --added