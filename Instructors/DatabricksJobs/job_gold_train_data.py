# Databricks notebook source
# MAGIC %md
# MAGIC ##SETUP

# COMMAND ----------

UC_enabled = False
reset = False

# COMMAND ----------

# MAGIC %run ../utils/setup

# COMMAND ----------

silver_table_name = silver_table_name+ "_job"
gold_table_name = gold_table_name+ "_job"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start
# MAGIC ##Make sure you ran SETUP first

# COMMAND ----------

df_silver = spark.read.table(silver_table_name)

# COMMAND ----------

# hands on excercise make df_silver available to use in SQL queries, call the temporary view silver_temp_view
df_silver.createOrReplaceTempView("silver_temp_view")

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS {gold_table_name} (stop_id STRING, current_status STRING, total INT);""")

# COMMAND ----------

spark.sql(f"""INSERT INTO {gold_table_name} SELECT current_status, stop_id, count(*) as total FROM silver_temp_view GROUP BY ALL;""")