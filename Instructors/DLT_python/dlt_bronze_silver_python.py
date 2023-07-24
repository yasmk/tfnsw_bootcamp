# Databricks notebook source
# import sys, os
# sys.path.append(os.path.abspath('/Repos/yas.mokri@databricks.com/tfnsw_bootcamp/'))

import dlt
from pyspark.sql.protobuf.functions import from_protobuf
from pyspark.sql.functions import col, explode

# COMMAND ----------

input_path = spark.conf.get("mypipeline.input_path")
input_path = input_path.strip(' ')

@dlt.table(name="dlt_bronze")
def bronze_table():
  bronze_df = (
      spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "parquet")  
      .load(input_path)
  )
  return bronze_df


# COMMAND ----------

# MAGIC %md
# MAGIC ## create silver using what you've learned so far and your previous code for unpacking proto_df
# MAGIC - create a live table
# MAGIC - read from a live table  

# COMMAND ----------

@dlt.view()
def bronze_view():

    df_bronze = dlt.read_stream("dlt_bronze")

    descriptor_file = "/dbfs/FileStore/tmp/transport_bootcamp/desc/gtfs-realtime_1007_extension.desc"


    proto_df = df_bronze.select(col("timestamp").alias("ingest_time") , from_protobuf(df_bronze.data, "FeedMessage", descFilePath=descriptor_file).alias("proto"))

    unpacked_df = proto_df.select('ingest_time', 'proto.*').select('ingest_time', explode(col('entity')).alias("entity"))

    unpacked_df = unpacked_df.select('ingest_time', "entity", "entity.*").select('ingest_time', "entity", "id", "alert","vehicle.*")

    return unpacked_df

# COMMAND ----------

@dlt.table()
def dlt_silver():

  unpacked_df = dlt.read_stream("bronze_view")

  return unpacked_df

# COMMAND ----------

# MAGIC %md
# MAGIC #SCD with DLT

# COMMAND ----------

dlt.create_target_table(
  name="dlt_silver_scd",
  table_properties = {"caseSensitive" : "true", "delta.enableChangeDataFeed": "true"}
)

dlt.apply_changes(
  target = "dlt_silver_scd",
  source = "bronze_view",
  keys = ["id"],
  sequence_by = "ingest_time", # or a btter timestamp
  stored_as_scd_type = 2
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Let's do this in SQL (open the gold notebook from the repo with SQL as the language)
# MAGIC