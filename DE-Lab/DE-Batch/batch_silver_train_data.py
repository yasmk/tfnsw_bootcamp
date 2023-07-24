# Databricks notebook source
# MAGIC %md
# MAGIC ##SETUP

# COMMAND ----------

UC_enabled = False
reset = False

# COMMAND ----------

# MAGIC %run ../utils/setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start
# MAGIC ##Make sure you ran SETUP first

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Read protobuf data from the Bronze table

# COMMAND ----------

df_bronze = spark.read.table(bronze_table_name)
display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse protobuf data using fom_protobuf 

# COMMAND ----------

from pyspark.sql.protobuf.functions import from_protobuf
from pyspark.sql.functions import col, explode

proto_df = df_bronze.select(col("timestamp").alias("ingest_time") , from_protobuf(df_bronze.data, "FeedMessage", descFilePath=descriptor_file).alias("proto"))

display(proto_df)


# COMMAND ----------

proto_df.schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## unpack struct data into columns 

# COMMAND ----------

unpacked_df = proto_df.select('ingest_time', 'proto.*').select('ingest_time', explode(col('entity')).alias("entity"))

# hands on exercise- continue by unpacking some more fields like entity then vehicle, use a pattern similar proto_df.select('proto.*')
unpacked_df = unpacked_df.select ##---- fill in the rest

display(unpacked_df)

# COMMAND ----------

# you can do something similar using SQL as well. Then you can use unpacked_df = spark.sql("you_sql_query") to acheive the same results
#https://docs.databricks.com/optimizations/semi-structured.html

# COMMAND ----------

proto_df.createOrReplaceTempView("proto_temp_view")

# COMMAND ----------

dbutils.widgets.text('database', database)
dbutils.widgets.text('bronze_table', bronze_table_name)
dbutils.widgets.text('silver_table', silver_table_name)
dbutils.widgets.text('gold_table', gold_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select entity.* from (select (explode(proto.entity)) as entity from proto_temp_view)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Write to silver
# MAGIC Write the results to the silver table (you can use silver_table_name) using the method we used earlier to write to Bronze then use a SQL statement to verify the results

# COMMAND ----------

print(silver_table_name)

# COMMAND ----------

unpacked_df.write ##---- fill in the rest


# COMMAND ----------

# MAGIC %md
# MAGIC ##Schema Evolution
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from $silver_table

# COMMAND ----------

# MAGIC %md
# MAGIC ###Unpack one more field and Append to Silver Table

# COMMAND ----------

unpacked_df = unpacked_df.select('ingest_time', "entity", "id", "alert","vehicle.*")
display(unpacked_df)

# COMMAND ----------

unpacked_df.write.mode('append').option("mergeSchema", "true").saveAsTable(silver_table_name)


# COMMAND ----------

# MAGIC %md
# MAGIC Check that table now has the extra columns for the new rows added

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from $silver_table WHERE stop_id IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC Without reprocessing the existing data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from $silver_table WHERE stop_id IS  NULL

# COMMAND ----------

# MAGIC %md 
# MAGIC ##DML
# MAGIC Update congestion_level from UNKNOWN_CONGESTION_LEVEL to another value for one or some of the rows in the table
# MAGIC Write a SQL query to do this either by using %sql or spark.sql(sql_query)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, congestion_level FROM $silver_table WHERE id in (1,2,10);

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE $silver_table SET congestion_level="VERY_KNOWN_CONGESTION_LEVEL" WHERE id in (1,2,10) ;
# MAGIC SELECT id, congestion_level FROM $silver_table WHERE id in (1,2,10);

# COMMAND ----------

# MAGIC %md 
# MAGIC #let's check the history of the table

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history $silver_table

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail $silver_table

# COMMAND ----------

# MAGIC
# MAGIC %ls /dbfs/user/hive/warehouse/yas_mokri_bootcamp.db/silver_train_data
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT id, congestion_level FROM $silver_table  VERSION AS OF -- specify the version before the latest change  WHERE id in (1,2,10) 

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT id, congestion_level FROM $silver_table  VERSION AS OF -- specify the laest version WHERE id in (1,2,10)

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE $silver_table VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT id, congestion_level FROM $silver_table  WHERE id in (1,2,10)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history $silver_table
