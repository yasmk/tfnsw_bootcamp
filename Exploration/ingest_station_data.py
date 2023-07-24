# Databricks notebook source
# MAGIC %run ../Instructors/utils/setup

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC wget -O nsw_train_stations.geojson "https://portal.spatial.nsw.gov.au/server/rest/services/NSW_FOI_Transport_Facilities/FeatureServer/1/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=*&returnGeometry=true&maxAllowableOffset=&geometryPrecision=&outSR=&havingClause=&gdbVersion=&historicMoment=&returnDistinctValues=false&returnIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&multipatchOption=xyFootprint&resultOffset=&resultRecordCount=&returnTrueCurves=false&returnExceededLimitFeatures=false&quantizationParameters=&returnCentroid=false&sqlFormat=none&resultType=&featureEncoding=esriDefault&datumTransformation=&f=geojson"
# MAGIC mkdir -p dbfs/$datasets_location
# MAGIC cp nsw_train_stations.geojson dbfs/$datasets_location
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# let's read the json output and the file metadata https://docs.databricks.com/ingestion/file-metadata-column.html

df_bronze = (spark.read
    .option("multiline", "true")
    .format("json")
    .load(f"dbfs:/nsw_train_stations.geojson")
    .select("type", explode(col("features")).alias("feature"), col("_metadata.file_modification_time").alias("timestamp"))
    .write.saveAsTable(f"{database}.bronze_station_data"))

# COMMAND ----------

df_silver = (spark.read
             .table(f"{database}.bronze_station_data")
             .select("timestamp", col("feature.properties").alias("properties"), to_json(col("feature.geometry")).alias("geometry"))
             .select("timestamp", "properties.*", "geometry")
             .write.saveAsTable(f"{database}.silver_station_data"))


# COMMAND ----------

# filter train stations only - exclude Light Rail and Ski stops
df_gold = (spark.read
             .table(f"{database}.silver_station_data")
             .select("*")
             .filter(~(col("generalname").like("%STOP%") | col("generalname").like("%skitube%")))
             .write.saveAsTable(f"{database}.gold_station_data"))
