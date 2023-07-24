# Databricks notebook source
# MAGIC %md
# MAGIC ## SETUP

# COMMAND ----------

UC_enabled = False
reset = True

# COMMAND ----------

# MAGIC %run ../utils/setup

# COMMAND ----------

import time
import requests
from datetime import datetime

# COMMAND ----------

def get_sydney_trains_data():
    # API endpoint URL
    url = api_uri

    # Set the required headers
    headers = {
        'Authorization': f'apikey {dbutils.secrets.get(scope=scope_name, key="opendata_apikey")}',
        'Accept': 'application/x-google-protobuf'
    }

    # Make the API call
    data = None
    try:
      response = requests.get(url, headers=headers)

      response.raise_for_status() # this will raise an exception if using dataflow we can retry the api call 

      data = response.content
    except Exception as e:
      raise (e)

    return data


# COMMAND ----------

sleep_time = 10
output_path = f"{datasets_location}/apidata/"

# while True:
# for i in range(0,2) 
api_data = get_sydney_trains_data()
data = [{
  "source": "api_transport_nsw",
  "timestamp": datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%S"),
  "data": api_data,
}]

df = spark.createDataFrame(data=data)

##write the data into cloud file storage as parquet
## df.write.mode('append').option("mergeSchema", "true").saveAsTable(bronze_table_name)
df.write.mode('append').parquet(output_path)
# time.sleep(sleep_time)
