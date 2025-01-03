-- Databricks notebook source
use catalog hive_metastore

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/adlsf1/presentation"

-- COMMAND ----------

DESC DATABASE f1_presentation;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC head()

-- COMMAND ----------

describe extended f1_raw.circuits

-- COMMAND ----------

describe extended f1_processed.circuits

-- COMMAND ----------

-- DBTITLE 1,dbfs:/mnt/adlsf1/processed/circuits
-- MAGIC %fs 
-- MAGIC ls /mnt/adlsf1/processed/circuits

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC ls /mnt/adlsf1/processed

-- COMMAND ----------

select * from f1_processed.circuits