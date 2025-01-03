-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Lesson Objectives
-- MAGIC 1. Spark SQL documentation
-- MAGIC 1. Create Database demo
-- MAGIC 1. Data tab in the UI
-- MAGIC 1. SHOW command
-- MAGIC 1. DESCRIBE command
-- MAGIC 1. Find the current database

-- COMMAND ----------

use catalog hive_metastore;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW databases;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo1;

-- COMMAND ----------

DESCRIBE DATABASE demo1; 

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo; 

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Managed Tables
-- MAGIC ##### Learning Objectives
-- MAGIC 1. Create managed table using Python
-- MAGIC 1. Create managed table using SQL
-- MAGIC 1. Effect of dropping a managed table
-- MAGIC 1. Describe table 

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_results_python;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.format("parquet").load("dbfs:/user/hive/warehouse/demo.db/race_results_python").display()

-- COMMAND ----------

SELECT *
  FROM demo.race_results_python
 WHERE race_year = 2020;

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
AS
SELECT *
  FROM demo.race_results_python
 WHERE race_year = 2020;

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

DESC EXTENDED demo.race_results_sql;

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### External Tables
-- MAGIC ##### Learning Objectives
-- MAGIC 1. Create external table using Python
-- MAGIC 1. Create external table using SQL
-- MAGIC 1. Effect of dropping an external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP
)
USING parquet
LOCATION "abfss://presentation@adlsf1.dfs.core.windows.net/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py WHERE race_year = 2020;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - --CTAS AS MANAGED SQL TABLE
-- MAGIC - --INSERT INTO  FOR EXTERNAL TABLE

-- COMMAND ----------

SELECT COUNT(1) FROM demo.race_results_ext_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

describe extended demo.race_results_ext_sql

-- COMMAND ----------

DROP TABLE demo.race_results_sql

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Views on tables
-- MAGIC ##### Learning Objectives
-- MAGIC 1. Create Temp View
-- MAGIC 1. Create Global Temp View
-- MAGIC 1. Create Permanent View

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS
SELECT *
  FROM demo.race_results_python
 WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM v_race_results;

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
SELECT *
  FROM demo.race_results_python
 WHERE race_year = 2012;

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results
AS
SELECT *
  FROM demo.race_results_python
 WHERE race_year = 2000;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

SELECT * FROM demo.pv_race_results;

-- COMMAND ----------

