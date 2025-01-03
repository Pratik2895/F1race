-- Databricks notebook source
USE CATALOG hive_metastore

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "abfss://processed@adlsf1.dfs.core.windows.net/"

-- COMMAND ----------

DESC DATABASE f1_processed;

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "abfss://presentation@adlsf1.dfs.core.windows.net/"