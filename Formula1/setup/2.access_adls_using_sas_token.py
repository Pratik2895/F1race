# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config for SAS Token
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

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

display(dbutils.fs.ls("abfss://demo@adlsf1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@adlsf1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

spark.read.format("csv").option("header", "true").load("abfss://demo@adlsf1.dfs.core.windows.net/circuits.csv").display()