# Databricks notebook source
# MAGIC %md
# MAGIC ## SETUP

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
# MAGIC
# MAGIC ### Load files incrementally from input_path (api output is stored here)

# COMMAND ----------

input_path = f"{datasets_location}/apidata/"
schema_path = f"{datasets_location}/schema/"

bronze_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet") 
    .option("cloudFiles.schemaLocation",schema_path) 
    .load(input_path)
)
display(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Store the data into Bronze

# COMMAND ----------

checkpoint_location = f"{datasets_location}/checkpoints/{bronze_table_name}"
bronze_df.writeStream.option("mergeSchema", "true").option(
    "checkpointLocation", checkpoint_location
).table(bronze_table_name)