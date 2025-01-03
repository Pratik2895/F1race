# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader API

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hive_metastore;

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

raw_folder_path 

# COMMAND ----------

dbutils.widgets.text("p_data_source", "testing")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True) 
])

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Add ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))\
.withColumn("data_source", lit(v_data_source))\
.withColumn("file_date", lit(v_file_date))  

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Select only the columns required & rename as required

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), 
                                                   col('round'), col('circuitId').alias('circuit_id'),col('name'), col('ingestion_date'), col('race_timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write the output to processed container in parquet format

# COMMAND ----------

# races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet('abfss://processed@adlsf1.dfs.core.windows.net/races')

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hive_metastore

# COMMAND ----------

races_selected_df.write.mode("overwrite").format("delta").partitionBy('race_year').saveAsTable('f1_processed.races')

# COMMAND ----------

# MAGIC %sql
# MAGIC select  count(*) from f1_processed.races
# MAGIC