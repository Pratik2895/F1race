# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use catalog hive_metastore

# COMMAND ----------

dbutils.widgets.text("p_data_source", "testing")
v_data_source = dbutils.widgets.get("p_data_source")
dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_ddl_schema = """
    raceId INT NOT NULL,
    driverId INT,
    stop STRING,
    lap INT,
    time STRING,
    duration STRING,
    milliseconds INT
"""

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())\
.withColumn("data_source", lit(v_data_source))\
.withColumn("file_date", lit(v_file_date))  


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

# overwrite_partition(final_df, 'f1_processed', 'pit_stops', 'race_id')

# COMMAND ----------

# final_df.write.mode("overwrite").partitionBy('race_id').parquet("abfss://processed@adlsf1.dfs.core.windows.net/pit_stops")

# COMMAND ----------

# spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")
# from delta.tables import DeltaTable
# if spark._jsparkSession.catalog().tableExists("hive_metastore.f1_processed.pit_stops"):
#     deltaTable = DeltaTable.forPath(spark, "abfss://processed@adlsf1.dfs.core.windows.net/pit_stops")
#     deltaTable.alias("tgt") \
#         .merge(
#             final_df.alias("src"),
#             "tgt.result_id = src.result_id"
#         ) \
#         .whenMatchedUpdateAll() \
#         .whenNotMatchedInsertAll() \
#         .execute()
# else:
#     final_df.write \
#         .mode("append") \
#         .partitionBy("race_id") \
#         .format("delta") \
#         .saveAsTable("hive_metastore.f1_processed.pit_stops")

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hive_metastore

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable('f1_processed.pit_stops')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT file_date, count(*) FROM f1_processed.pit_stops
# MAGIC GROUP BY 1;