-- Databricks notebook source
SELECT * FROM odl_user_1016514_bootcamp.dlt_silver_scd

-- COMMAND ----------

SELECT ingest_time, id, `__START_AT` as start_at, `__END_AT` as end_at FROM odl_user_1016514_bootcamp.dlt_silver_scd where id = 1

-- COMMAND ----------

select * from odl_user_1016514_bootcamp.dlt_silver where id=1