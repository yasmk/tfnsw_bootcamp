# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # How do I work with Unstructured Data?
# MAGIC
# MAGIC There are many use cases requiring data teams to work with non-tabular data:
# MAGIC - Running machine learning on large collections of unstructured data such as image, audio, video, or PDF files.
# MAGIC - Persisting and sharing training, test, and validation data sets used for model training and defining locations for operational data such as logging and checkpointing directories.
# MAGIC - Uploading and querying non-tabular data files in data exploration stages in data science.
# MAGIC - Working with tools that don't natively support Cloud object storage APIs and instead expect files in the local file system on cluster machines.
# MAGIC Storing and providing secure access across workspaces to libraries, certificates, and other configuration files of arbitrary formats, such as .whl or .txt, before they are used to configure cluster libraries, notebook-scoped libraries, or job dependencies.
# MAGIC - Staging and pre-processing raw data files in the early stages of an ingestion pipeline before they are loaded into tables, e.g., using Auto Loader or COPY INTO.
# MAGIC - Sharing large collections of files with other users within or across workspaces, regions, clouds, and data platforms.
# MAGIC
# MAGIC ## Introducing Volumes...
# MAGIC Volumes allow you to manage and work with unstructured and non-tabluar datasets in Databricks. Once you have a Volume setup you have the capabilities for accessing, storing, and managing data in any format, including structured, semi-structured, and unstructured data. This enables you to govern, manage and track lineage for non-tabular data along with the tabular data and models in Unity Catalog, providing a unified discovery and governance experience.
# MAGIC
# MAGIC <p>
# MAGIC
# MAGIC <img src="https://cms.databricks.com/sites/default/files/inline-images/db-696-blog-img-1.gif" width=700>
# MAGIC <p>
# MAGIC <p>
# MAGIC
# MAGIC You can then work with these directories directly in the Notebook or browse the files in your Data Explorer.
# MAGIC <p>
# MAGIC <img src='https://cms.databricks.com/sites/default/files/inline-images/db-696-blog-img-2.gif' width=700>

# COMMAND ----------

volume = "/Volumes/lisa_sherin_dac_demo_catalog/ref_timetables/lisa_image_volumes" # where I have stored image data

catalog = 'lisa_sherin_dac_demo_catalog'
schema = 'ref_timetables'
table_name = "bronze_image_data"
checkpoint_location = f"/tmp/checkpoints/{table_name}"
schema_location = "/tmp/schema/"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Loading unstructured data incrementally
# MAGIC
# MAGIC ## Delta & Auto-loader to the rescue
# MAGIC Databricks Autoloader provides native support for images and binary files.
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/autoloader-images.png" width="800" />

# COMMAND ----------

autoloader_image_df = (spark.readStream
                      .format("cloudFiles")
                      .option("cloudFiles.format", "binaryFile")
                      .option("cloudFiles.schemaLocation", schema_location+"schema_bronze_images")
                      .option("cloudFiles.maxFilesPerTrigger", 10000)
                      .option("pathGlobFilter", "*.jpg")
                      .load(volume)).select("*", "_metadata")

(autoloader_image_df.writeStream
              .trigger(availableNow=True)
              .option("checkpointLocation", checkpoint_location)
              .table(f"{catalog}.{schema}.{table_name}").awaitTermination())

display(spark.table(f"{catalog}.{schema}.{table_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Loading unstructured data as a full batch

# COMMAND ----------

batch_image_df = (spark.read
            .format("image")
            .option("pathGlobFilter", "*.jpg")
            .option("dropInvalid", value = True)
            .load(path=volume)
            .select("*", "_metadata"))

batch_image_df.display()
