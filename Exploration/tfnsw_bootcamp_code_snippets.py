# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Register a Python UDF for SQL
# MAGIC
# MAGIC Make predictive analytics and advanced functions available for SQL users.
# MAGIC - Include predictive analytics in reports
# MAGIC - Include forecasting, etc as part of a data processing or data modelling task
# MAGIC
# MAGIC Can we show this in DLT?
# MAGIC - examples hashing functions, mapping functions? (thinking for real-time use case)

# COMMAND ----------

#load the model from the registry - replace model
get_status_udf = mlflow.pyfunc.spark_udf(spark, "models:/megacorp_team2_june2023/production")
#define the model as a SQL function to be able to call it in SQL
spark.udf.register("get_status_udf", get_status_udf)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- call the model in SQL using the udf registered as function
# MAGIC CREATE TABLE IF NOT EXISTS demo_turbine_team2.turbine_preds
# MAGIC AS 
# MAGIC select *, get_status_udf(struct(AN3, AN4, AN5, AN6, AN7, AN8, AN9, AN10)) as status_forecast 
# MAGIC from turbine_gold_for_ml

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Process Unstructured Data
# MAGIC - Process image/Lidar data (can we process metadata)
# MAGIC - Rebuild Demo - [PDF extract notebook](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#notebook/4156927442725646/command/539516573672045)