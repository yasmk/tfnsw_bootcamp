# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Reference Data Ingestion: OpenData Static Data
# MAGIC
# MAGIC In our OpenData example, we download zip files directly to our cloud storage (Volumes). We write raw data files (zip) to our designated cloud storage, unzip and explore the contents, convert each file to a delta table registered in the `reference_schema` database.
# MAGIC
# MAGIC In this example, I read from Volumes but for the bootcamp you also have the option to retrieve the files from upload the files via the UI.

# COMMAND ----------

# explore contents of the volume, you can also explore in the Data Explorer UI
file_list = dbutils.fs.ls(volume_path)

# COMMAND ----------

# create a table based on each reference file in the volume
for file in file_list:
    table_name = file.name.split('.')[0]
    df = spark.read.format('csv').option("header",'true').option("inferSchema", 'true').load(file.path)
    df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.{table_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE catalog lisa_sherin_dac_demo_catalog;
# MAGIC USE database ref_timetables;
# MAGIC SHOW TABLES;
