# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### This Notebook is only to help you set up the DLT pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Configure DLT Pipeline
# MAGIC
# MAGIC Pipeline code is stored in a different notebook. This notebook will help you get some custom values needed to create DLT Pipeline.
# MAGIC
# MAGIC To run this lab we need to use standardized values for  **Target** and  **Storage Location** and **Configuration** .
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC Run the cell bellow and use values returned. They will be specific to your lab environment.

# COMMAND ----------

UC_enabled = False
reset = True

# COMMAND ----------

# MAGIC %run ../utils/setup

# COMMAND ----------

storage_location = f'{datasets_location}dlt_pipeline'
pipline_name = f"{database_name}_train_data_pipeline"
notebook_path = f"/Repos/{current_user_id}/tfnsw_bootcamp/Instructors/DLT_python/dlt_bronze_silver_python"
input_path = f"{datasets_location}dlt_apidata/"
cluster_mode = "Fixed size"
workers = 1

displayHTML("""<h2>Use these values to create your Delta Live Pipeline</h2>""")
displayHTML("""<b>Pipeline name: </b>""")
displayHTML(f"""<b style="color:green">{pipline_name}</b>""")
displayHTML("""<b>Source code paths: </b>""")
displayHTML(f"""<b style="color:green">{notebook_path}</b>""")
displayHTML("""<b>Storage Location: </b>""")
displayHTML("""<b style="color:green">{}</b>""".format(storage_location))
displayHTML("""<b>Target Schema:</b>""")
displayHTML("""<b style="color:green">{}</b>""".format(database_name))
displayHTML("""<b>Cluster policy:</b>""")
displayHTML("""<b style="color:green">{}</b>""".format("DBAcademy-DLT-Policy"))
displayHTML("""<b>Cluster mode:</b>""")
displayHTML("""<b style="color:green">{}</b>""".format(cluster_mode))
displayHTML("""<b>Workers:</b>""")
displayHTML("""<b style="color:green">{}</b>""".format(workers))

displayHTML("""<b>Advanced -> Configuration:</b>""")
displayHTML("""Key: <b style="color:green">mypipeline.input_path</b>""")
displayHTML("""Value: <b style="color:green">{}</b>""".format(input_path))

displayHTML("""<b>Channel:</b>""")
displayHTML("""<b style="color:green">{}</b>""".format("Preview"))



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Incremental Updates

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Simulate new batch files being uploaded to cloud location. You can run it multiple times - it will generate a sample of orders for randomly selected store.
# MAGIC
# MAGIC If pipeline is running in continuous mode - files will be processed as soon as they are uploaded. Otherwise new files will be picked up on the next run.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Explore run Logs

# COMMAND ----------

spark.sql(f"USE {database_name};")

spark.sql(f"CREATE OR REPLACE VIEW pipeline_logs AS SELECT * FROM delta.`{storage_location}/system/events`")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended pipeline_logs

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   id,
# MAGIC   timestamp,
# MAGIC   expectations.dataset,
# MAGIC   expectations.name,
# MAGIC   expectations.failed_records,
# MAGIC   expectations.passed_records
# MAGIC FROM(
# MAGIC   SELECT 
# MAGIC     id,
# MAGIC     timestamp,
# MAGIC     details:flow_progress.metrics,
# MAGIC     details:flow_progress.data_quality.dropped_records,
# MAGIC     explode(from_json(details:flow_progress:data_quality:expectations
# MAGIC              ,schema_of_json("[{'name':'str', 'dataset':'str', 'passed_records':42, 'failed_records':42}]"))) expectations
# MAGIC   FROM pipeline_logs
# MAGIC   WHERE details:flow_progress.metrics IS NOT NULL
# MAGIC   ) data_quality
# MAGIC   ORDER BY timestamp desc
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC The `details` column contains metadata about each Event sent to the Event Log. There are different fields depending on what type of Event it is. Some examples include:
# MAGIC * `user_action` Events occur when taking actions like creating the pipeline
# MAGIC * `flow_definition` Events occur when a pipeline is deployed or updated and have lineage, schema, and execution plan information
# MAGIC   * `output_dataset` and `input_datasets` - output table/view and its upstream table(s)/view(s)
# MAGIC   * `flow_type` - whether this is a complete or append flow
# MAGIC   * `explain_text` - the Spark explain plan
# MAGIC * `flow_progress` Events occur when a data flow starts running or finishes processing a batch of data
# MAGIC   * `metrics` - currently contains `num_output_rows`
# MAGIC   * `data_quality` - contains an array of the results of the data quality rules for this particular dataset
# MAGIC     * `dropped_records`
# MAGIC     * `expectations`
# MAGIC       * `name`, `dataset`, `passed_records`, `failed_records`
# MAGIC   
