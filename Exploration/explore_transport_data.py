# Databricks notebook source
# MAGIC %md
# MAGIC # SETUP

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC For some exploration of our datasets, and to make some nice visualisations we need to follow the steps below.
# MAGIC
# MAGIC 1. Import Databricks columnar functions (including H3) for DBR / DBSQL Photon with `from pyspark.databricks.sql.functions import *`
# MAGIC
# MAGIC 2. To use [KeplerGl](https://kepler.gl/) OSS library for map layer rendering:
# MAGIC   * Import with `from keplergl import KeplerGl` to use directly
# MAGIC
# MAGIC __Note: If you hit `H3_NOT_ENABLED` [[docs](https://docs.databricks.com/error-messages/h3-not-enabled-error-class.html#h3_not_enabled-error-class)]__
# MAGIC
# MAGIC > `h3Expression` is disabled or unsupported. Consider enabling Photon or switch to a tier that supports H3 expressions. [[AWS](https://www.databricks.com/product/aws-pricing) | [Azure](https://azure.microsoft.com/en-us/pricing/details/databricks/) | [GCP](https://www.databricks.com/product/gcp-pricing)]
# MAGIC
# MAGIC __Recommend running on DBR 11.3+ for maximizing photonized functions.__

# COMMAND ----------

# MAGIC %pip install keplergl --quiet

# COMMAND ----------

UC_enabled = False
reset = False # we don't want to drop our tables from earlier

# COMMAND ----------

# MAGIC %md
# MAGIC Let's ingest some additional geospatial data for each of the stations in NSW

# COMMAND ----------

# MAGIC %run ../Exploration/ingest_station_data

# COMMAND ----------

# MAGIC %md
# MAGIC # START

# COMMAND ----------

from pyspark.databricks.sql.functions import *

stations = spark.read.table(f'{database}.gold_station_data')

# COMMAND ----------

# let's get our train data from before
table = f'{database}.silver_train_data'

df = spark.read.table(table)

# drop timestamp, trip_id duplicates
trains = (df.orderBy("trip_id")
          .dropDuplicates(subset = ['timestamp', 'trip_id'])
)

trains.display()

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
# map_1.add_data(data=trains.toPandas(), name="train_position")
map_1.add_data(data=stations.select("generalname", "urbanity", "geometry").toPandas(), name='stations')
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

# trains stopped for >3 min
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

