# Databricks notebook source
# MAGIC %md
# MAGIC Access Azure Data Lake using access keys
# MAGIC - 1.Set the spark config fs.azure.account.key
# MAGIC - 2.List files from demo container
# MAGIC - 3.Read data from circuits.csv file

# COMMAND ----------

# DBTITLE 1,sample code
spark.conf.set(
    "fs.azure.account.key.<storage-account>.dfs.core.windows.net",
    dbutils.secrets.get(scope="<scope>", key="<storage-account-access-key>"))

# COMMAND ----------

keyparam = dbutils.secrets.get(scope = 'adlsf1scope', key = 'adlsf1accountaccesskey')

# COMMAND ----------

# DBTITLE 1,usingkey url
spark.conf.set(
    "fs.azure.account.key.adlsf1.dfs.core.windows.net",
    keyparam )

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.adlsf1.dfs.core.windows.net",
    "rdkhmt0MlRLjExZNTsc5HeShp2NwtndQjMPhL0vLFwRxm1aYtq7P0O+hk4IOV/Tu/s68HDbIwzdJ+AStf5mLEg==")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@adlsf1.dfs.core.windows.net/"))

# COMMAND ----------

spark.read.format("csv").option("header", "true").load("abfss://demo@adlsf1.dfs.core.windows.net/circuits.csv").display()