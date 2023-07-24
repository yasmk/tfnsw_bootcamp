# Databricks notebook source
# MAGIC %pip install keplergl --quiet

# COMMAND ----------

from pyspark.databricks.sql.functions import *
from pyspark.sql.functions import *


# COMMAND ----------

table = 'lisa_sherin_dac_demo_catalog.ref_timetables.realtime_tripfeed'

df = spark.read.table(table)

# drop timestamp, trip_id duplicates
trains = (df.orderBy("trip_id")
          .dropDuplicates(subset = ['timestamp', 'trip_id'])
)

trains.display()

# COMMAND ----------

# read in stations but exclude Light Rail and Ski stops
stations = spark.read.table('lisa_nsw.nsw_train_stations').filter(~(col("generalname").like("%STOP%") | col("generalname").like("%skitube%")))

stations.select("classsubtype", "generalname").display()

# COMMAND ----------

from keplergl import KeplerGl
def display_kepler(kmap:KeplerGl, height=800, width=1200) -> None:
  """
  Convenience function to render map in kepler.gl
  - use this when cannot directly render or
    want to go beyond the %%mosaic_kepler magic.
  """
  displayHTML(
    kmap
      ._repr_html_()
      .decode("utf-8")
      .replace(".height||400", f".height||{height}")
      .replace(".width||400", f".width||{width}")
  )

# COMMAND ----------

map_1 = KeplerGl(height=600, config={'mapState': {'latitude': -33.85, 'longitude': 151.1, 'zoom': 10}})
map_1.add_data(data=trains.toPandas(), name="train_position")
map_1.add_data(data=stations.select("generalname", "geom_wkt").toPandas(), name='stations')
display_kepler(map_1)

# COMMAND ----------

h3_resolution = 11

df_hex11 =  (
    trains
        .withColumn("h3_resolution", lit(h3_resolution))
        .withColumn("hex_id", h3_longlatash3("longitude", "latitude", h3_resolution))
        .withColumn("hex_centroid", h3_centeraswkt("hex_id"))
    .select(*trains.columns,"hex_id","hex_centroid","h3_resolution")
)

# COMMAND ----------

# trains stopped for >3 min and not within 100m of a station
df_hex11_dense = (
    df_hex11
        .groupBy("latitude", "longitude", "trip_id")
        .count()
        .filter("count > 2")
        .orderBy(desc("count"))
)

# COMMAND ----------

df_hex11_dense.display()

# COMMAND ----------

map_4 = KeplerGl(height=600, config={'mapState': {'latitude': -33.85, 'longitude': 151.1, 'zoom': 10}})
map_4.add_data(data=stations.select("generalname", "geom_wkt").toPandas(), name='stations')
map_4.add_data(data = df_hex11_dense.select("latitude", "longitude", "trip_id", col("count").alias("mins_wait")).toPandas(), name = "hex11_dense_waits")
display_kepler(map_4)
