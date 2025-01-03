-- Databricks notebook source
use catalog hive_metastore

-- COMMAND ----------

describe database f1_processed_managed

-- COMMAND ----------

-- DBTITLE 1,external_database
describe database f1_raw

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/adlsf1/processed"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed_v1
LOCATION "/mnt/pratikf1/processed"

-- COMMAND ----------

DESC DATABASE f1_processed;

-- COMMAND ----------

describe extended f1_processed.circuits

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