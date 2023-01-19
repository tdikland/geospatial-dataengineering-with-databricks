# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Mosaic for Geospatial Functions
# MAGIC 
# MAGIC The Mosaic is a databricks-labs project, that intends to simplify the implementation of scalable geospatial data pipelines by bounding together common Open Source geospatial libraries via Apache Spark, with a set of examples and best practices for common geospatial use cases. In this Notebook we'll cover how to setup Mosaic in Databricks.
# MAGIC 
# MAGIC ## Learning goals
# MAGIC After completing this notebook you should be able to:
# MAGIC - Install the databrickslabs/mosaic library on a Databricks cluster
# MAGIC - Configure the geometry api used for geospatial operations
# MAGIC - Register the geospatial functions in Spark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cluster configurations
# MAGIC In order to run mosaic on Databricks clusters, you must use DBR (Databricks runtime) version 10.0 or higher. In order to take full advantage of H3 grid indexing the minimum DBR version is 11.2 with photon enabled. The latter is highly recommended by Databricks. 

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Installation of Mosaic
# MAGIC You could download the release artifacts of mosaic from GitHub and attach them to a cluster manually. However in the case of python, one can also easily install it directly using the `%pip` magic command.

# COMMAND ----------

# MAGIC %pip install databricks-mosaic

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuring Mosaic backends
# MAGIC Mosaic depends on other libraries to do the hard work of relating geometries, indexing or converting rasters. You may choose to change these to be the best fit for your use case.
# MAGIC 
# MAGIC ### Geometry API
# MAGIC Determines which library should be used to do geometric operations. The current options are:
# MAGIC - (Default) ESRI (https://github.com/Esri/geometry-api-java)
# MAGIC - JTS (https://github.com/locationtech/jts)
# MAGIC 
# MAGIC ### Index System
# MAGIC Determines which index system should be used to "index" geometries. The current options are:
# MAGIC - (Default) H3 (https://h3geo.org/)
# MAGIC - BNG (https://en.wikipedia.org/wiki/Ordnance_Survey_National_Grid)
# MAGIC 
# MAGIC ### Raster API
# MAGIC Determine how rasters should be read and processed. The current options are:
# MAGIC - (Default) GDAL (https://github.com/OSGeo/gdal)

# COMMAND ----------

# Set GeometryAPI to `JTS` using spark configuration
spark.conf.set("spark.databricks.labs.mosaic.geometry.api", "JTS")

# Set Index System to  `BNG` using spark configuration
spark.conf.set("spark.databricks.labs.mosaic.index.system", "BNG")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enabling Mosaic functions
# MAGIC Before the functions from mosaic can be used, they first need to be registered in the spark context. This can be done using the the `enable_mosaic` method that is imported from the mosaic package

# COMMAND ----------

from mosaic import enable_mosaic
enable_mosaic(spark, dbutils)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ready, Set, Go!
# MAGIC Mosaic is now correctly setup and ready to do some work.
