# Databricks notebook source
# MAGIC %md
# MAGIC # Realtime Data Ingestion: OpenData Realtime Trip Data
# MAGIC Current vehicle positions in GTFS-realtime format for Buses, Ferries, Light Rail and Trains is available from [opendata](https://opendata.transport.nsw.gov.au/dataset/public-transport-realtime-vehicle-positions-v2). Using an API key activated in the website, we pull create a near-realtime streaming job, polling the API every 60 seconds for Train Locations.
# MAGIC
# MAGIC **Author:** Lisa Sherin lisa.sherin@databricks.com

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Initial Set-up Notes for Protobuf
# MAGIC
# MAGIC The python module is already available in the repo, this section is just for reference if the protobuf is updated.
# MAGIC
# MAGIC ### Pip Install Requirements
# MAGIC ```pip install protobuf```
# MAGIC
# MAGIC ### Bash Script to generate python module
# MAGIC ```
# MAGIC %sh 
# MAGIC
# MAGIC apt install -y protobuf-compiler
# MAGIC
# MAGIC protoc -I=. --python_out=. gtfs-realtime_1007_extension.proto_
# MAGIC ```

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install protobuf
# MAGIC
# MAGIC

# COMMAND ----------

# create widgets to parameterise the notebook
dbutils.widgets.text('target_catalog', 'INSERT_HERE', label='target_catalog')
dbutils.widgets.text('target_schema', 'INSERT_HERE', label='target_schema')
dbutils.widgets.text('target_table', 'INSERT_HERE', label='target_table')
dbutils.widgets.text('api_url', 'INSERT_HERE', label='api_url')

# COMMAND ----------

# retrieve variables passed to the notebook
target_catalog = dbutils.widgets.get('target_catalog')
target_schema = dbutils.widgets.get('target_schema')
target_table = dbutils.widgets.get('target_table')
api_url = dbutils.widgets.get('api_url')

# COMMAND ----------

# install protobuf parser
import gtfs_realtime_1007_extension.proto__pb2 as pb
import requests

def get_sydney_trains_data():
    # API endpoint URL
    url = 'https://api.transport.nsw.gov.au/v2/gtfs/vehiclepos/sydneytrains'

    # Set the required headers
    headers = {
        'Authorization': f'apikey {dbutils.secrets.get(scope="lisasherin", key="opendata_apikey")}',
        'Accept': 'application/x-google-protobuf'
    }

    # Make the API call
    response = requests.get(url, headers=headers)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Retrieve the data from the response
        data = response.content

        # Create a GTFS Realtime FeedMessage object
        feed = pb.FeedMessage()

        # Parse the protobuf data
        feed.ParseFromString(data)

        # Process the feed and create a list of dictionaries
        # update to include additional fields over time...
        data_list = []
        for entity in feed.entity:
            entity_data = {}
            if entity.HasField('id'):
                entity_data['id'] = entity.id

            if entity.HasField('is_deleted'):
                entity_data['is_deleted'] = entity.is_deleted

            if entity.HasField('trip_update'):
                trip_update = entity.trip_update
                if trip_update.HasField('trip'):
                    trip = trip_update.trip
                    entity_data['trip_id'] = trip.trip_id
                    entity_data['start_time'] = trip.start_time
                    entity_data['start_date'] = trip.start_date
                    entity_data['route_id'] = trip.route_id

                for stop_time_update in trip_update.stop_time_update:
                    stop_data = {}
                    stop_data['stop_id'] = stop_time_update.stop_id
                    stop_data['arrival_time'] = stop_time_update.arrival.time
                    stop_data['arrival_delay'] = stop_time_update.arrival.delay
                    stop_data['departure_time'] = stop_time_update.departure.time
                    stop_data['departure_delay'] = stop_time_update.departure.delay

                    entity_data.setdefault('stops', []).append(stop_data)

            if entity.HasField('vehicle'):
                vehicle = entity.vehicle
                if vehicle.HasField('trip'):
                    trip = vehicle.trip
                    entity_data['trip_id'] = trip.trip_id
                    entity_data['schedule_relationship'] = trip.schedule_relationship
                    entity_data['start_time'] = trip.start_time
                    entity_data['start_date'] = trip.start_date
                    entity_data['route_id'] = trip.route_id

                if vehicle.HasField('stop_id'):
                  entity_data['stop_id'] = vehicle.stop_id

                if vehicle.HasField('position'):
                    position = vehicle.position
                    entity_data['latitude'] = position.latitude
                    entity_data['longitude'] = position.longitude
                    entity_data['bearing'] = position.bearing
                  
                if vehicle.HasField('current_status'):
                  entity_data['current_status'] = vehicle.current_status

                if vehicle.HasField('timestamp'):
                  entity_data['timestamp'] = vehicle.timestamp

                if vehicle.HasField('congestion_level'):
                  entity_data['congestion_level'] = vehicle.congestion_level

                if vehicle.HasField('occupancy_status'):
                  entity_data['occupancy_status'] = vehicle.occupancy_status

            if entity.HasField('alert'):
                alert = entity.alert
                entity_data['alert_text'] = alert.header_text.translation[0].text
                entity_data['informed_entity'] = alert.informed_entity
                entity_data['cause'] = alert.cause
                entity_data['cause_detail'] = alert.cause_detail
                entity_data['effect'] = alert.effect
                entity_data['effect_detail'] = alert.effect_detail
                entity_data['description_text'] = alert.description_text

            data_list.append(entity_data)

        # Create a dataframe from the list of dictionaries
        df = spark.createDataFrame(data_list)
        return df
    else:
        # If the request was not successful, print the error message
        print(f'Request failed with status code {response.status_code}: {response.text}')

        return None

# COMMAND ----------

data = get_sydney_trains_data()
display(data)

# COMMAND ----------

import time

sleep_time = 60

while True: 
  data = get_sydney_trains_data()
  data.write.mode('append').option("mergeSchema", "true").saveAsTable("lisa_sherin_dac_demo_catalog.ref_timetables.realtime_tripfeed")
  print(f"Sleeping for {sleep_time} seconds")
  time.sleep(sleep_time)
