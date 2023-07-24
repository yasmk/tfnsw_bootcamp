# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Reference Data Ingestion: OpenData Static Data
# MAGIC
# MAGIC In our OpenData example, we download zip files directly to our cloud storage (Volumes). We write raw data files (zip) to our designated cloud storage, unzip and explore the contents, convert each file to a delta table registered in the `reference_schema` database. |

# COMMAND ----------

volume_path = '/Volumes/lisa_sherin_dac_demo_catalog/tfnsw_timetables/gtfs_open_data/'
catalog = 'lisa_sherin_dac_demo_catalog'
schema = 'ref_timetables'
apikey = dbutils.secrets.get(scope="lisasherin", key="opendata_apikey")
url = "https://opendata.transport.nsw.gov.au/dataset/timetables-complete-gtfs/resource/67974f14-01bf-47b7-bfa5-c7f2f8a950ca/download/gtfs.zip"

# COMMAND ----------

# MAGIC %md
# MAGIC # WIP - download of zip not working
# MAGIC
# MAGIC Need to add authentication to redirect

# COMMAND ----------

from subprocess import Popen

# todo can we make this programmatic
download_ref_data_cmd = (f""" curl -o={volume_path}/gtfs.zip \
                          "{url}" 
                          """
                        )
process = Popen(download_ref_data_cmd, shell=True)
process.wait()

# COMMAND ----------

# MAGIC %sh
# MAGIC wget -o /Volumes/lisa_sherin_dac_demo_catalog/ref_timetables/gtfs_open_data/gtfs.zip "https://opendata.transport.nsw.gov.au/node/544/download"
# MAGIC
# MAGIC ls /Volumes/lisa_sherin_dac_demo_catalog/ref_timetables/gtfs_open_data/

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip /Volumes/lisa_sherin_dac_demo_catalog/ref_timetables/gtfs_open_data/gtfs.zip -d /Volumes/lisa_sherin_dac_demo_catalog/ref_timetables/gtfs_open_data/

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /Volumes/lisa_sherin_dac_demo_catalog/ref_timetables/gtfs_open_data/gtfs.zip

# COMMAND ----------

# explore contents of the volume, you can also explore in the Data Explorar UI
file_list = dbutils.fs.ls(volume_path)

# COMMAND ----------

# create a table based on each reference file in the volume
for file in file_list:
  df = spark.read.format('csv').option("header",'true').option("inferSchema", 'true').load(file.path)
  df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.{file.name.split('.')[0]}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE catalog lisa_sherin_dac_demo_catalog;
# MAGIC USE database ref_timetables;
# MAGIC SHOW TABLES;

# COMMAND ----------

for table in list(_sqldf.select('tableName').collect()):
  spark.sql(f"DROP TABLE {table.tableName}")