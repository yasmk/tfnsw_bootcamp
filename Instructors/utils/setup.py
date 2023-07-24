# Databricks notebook source
current_user_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
datasets_location = f'/FileStore/tmp/{current_user_id}/'

catalog_name = "transport_bootcamp"
database_name = current_user_id.split('@')[0].replace('.','_')+'_bootcamp'

# COMMAND ----------

if UC_enabled:
  database = f"{catalog_name}.{database_name}"
else:
  database = f"{database_name}"
  
bronze_table_name = f"{database}.bronze_train_data"
silver_table_name = f"{database}.silver_train_data"
gold_table_name = f"{database}.gold_train_data"

# COMMAND ----------

api_uri = 'https://api.transport.nsw.gov.au/v2/gtfs/vehiclepos/sydneytrains'

# COMMAND ----------

source_descriptor_file = f"/Workspace/Repos/{current_user_id}/tfnsw_bootcamp/gtfs-realtime_1007_extension.desc"

descriptor_file = "/FileStore/tmp/transport_bootcamp/desc/gtfs-realtime_1007_extension.desc"

dbutils.fs.cp("file:"+source_descriptor_file, "dbfs:" +descriptor_file)

descriptor_file = '/dbfs'+descriptor_file

# COMMAND ----------

scope_name = "tfnsw_bootcamp"

# COMMAND ----------

if reset:
  dbutils.fs.rm(datasets_location, True)

  UC_enabled = False
  if UC_enabled:
    spark.sql(f'CREATE CATALOG IF NOT EXISTS {catalog_name}')

  spark.sql(f'drop database IF  EXISTS {database} cascade')
  spark.sql(f'create database if not exists {database};')
  spark.sql(f'use {database}')

  print (f"Created database :  {database}") 
