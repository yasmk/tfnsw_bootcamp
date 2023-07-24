-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## create gold using your previous code for gold in the batch pipeline
-- MAGIC  

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE total_status_count
AS SELECT current_status, stop_id, count(*) AS total
FROM LIVE.dlt_silver
GROUP BY ALL;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Next steps
-- MAGIC - Change the settings of your pipeline to include this notebook (source code-> Add this notebook)
-- MAGIC - Start the pipeline again