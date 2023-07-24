# Databricks notebook source
# MAGIC %md
# MAGIC ##SETUP

# COMMAND ----------

UC_enabled = False
reset = False

# COMMAND ----------

# MAGIC %run ../utils/setup

# COMMAND ----------

dbutils.widgets.removeAll()

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

df_silver = spark.read.table(silver_table_name)
display(df_silver)

# COMMAND ----------

# hands on excercise make df_silver available to use in SQL queries, call the temporary view silver_temp_view
df_silver.createOrReplaceTempView("silver_temp_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS $gold_table (stop_id STRING, current_status STRING, total INT);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO $gold_table 
# MAGIC SELECT current_status, stop_id, count(*) as total FROM silver_temp_view GROUP BY ALL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from $gold_table;

# COMMAND ----------

# MAGIC %md
# MAGIC #Another Gold Table? 
# MAGIC Come up with more menaingful aggregate based on the silver table and create your own Gold table

# COMMAND ----------

# MAGIC %md
# MAGIC ## How about a view this time? what's the difference?

# COMMAND ----------

# You can use this to enable merge schema 
# spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")