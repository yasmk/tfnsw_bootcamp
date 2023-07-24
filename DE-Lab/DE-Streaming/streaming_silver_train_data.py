# Databricks notebook source
# MAGIC %md
# MAGIC ##SETUP

# COMMAND ----------

UC_enabled = False
reset = False

# COMMAND ----------

# MAGIC %run ../utils/setup

# COMMAND ----------

dbutils.widgets.text('database', database)
dbutils.widgets.text('bronze_table', bronze_table_name)
dbutils.widgets.text('silver_table', silver_table_name)
dbutils.widgets.text('gold_table', gold_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start
# MAGIC ##Make sure you ran SETUP first

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Read protobuf data from the Bronze table

# COMMAND ----------

#df_bronze = spark.read.table(bronze_table_name)
df_bronze = spark.readStream.table(bronze_table_name)
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

# MAGIC %md
# MAGIC ## unpack struct data into columns 

# COMMAND ----------

unpacked_df = proto_df.select('ingest_time', 'proto.*').select('ingest_time', explode(col('entity')).alias("entity"))
unpacked_df = unpacked_df.select('ingest_time', "entity", "entity.*").select('ingest_time', "entity", "id", "alert","vehicle.*")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Write to silver
# MAGIC Write the results to the silver table (you can use silver_table_name) using the method we used earlier to write to Bronze then use a SQL statement to verify the results

# COMMAND ----------

print(silver_table_name)

# COMMAND ----------

#unpacked_df.write.mode('append').option("mergeSchema", "true").saveAsTable(silver_table_name)
checkpoint_location = f"{datasets_location}/checkpoints/{silver_table_name}"
unpacked_df.writeStream.option("mergeSchema", "true").option("checkpointLocation", checkpoint_location).table(silver_table_name)

# COMMAND ----------

display(spark.readStream.table(silver_table_name).where("id=1"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from $silver_table