# Databricks notebook source
# MAGIC %md
# MAGIC # Getting Started with Vector Geometries
# MAGIC 
# MAGIC this notebook provides a hands-on review on the basic construction, conversion and visualisation of geospatial vector data.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab you should be able to
# MAGIC - Express a spatial feature on Earth in a vector format
# MAGIC - Read vector data into Mosaic's internal format
# MAGIC - Convert between common vector formats
# MAGIC - Visualise geometries using kepler.gl 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Run the following script to setup the lab environment.

# COMMAND ----------

# MAGIC %run ../Includes/Setup-1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Represent your company HQ in WKT
# MAGIC 
# MAGIC In this notebook, we're going to work with the geometries of your company HQ. 
# MAGIC 
# MAGIC Use for instance [geojson.io](https://geojson.io) to find your company HQ on the map and draw a polygon.

# COMMAND ----------

# SOLUTION (AMS OFFICE)
company_hq_wkt = "POLYGON ((4.888677027629569 52.33591764757733,4.888677027629569 52.335759226198405,4.889275339651931 52.335759226198405,4.889275339651931 52.33591764757733,4.888677027629569 52.33591764757733))"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the geometry using Mosaic.
# MAGIC 
# MAGIC Run the following cell to create a spark dataframe holding your geometry in WKT format.

# COMMAND ----------

df = spark.createDataFrame([("company_hq", company_hq_wkt)], "id STRING, geom_wkt STRING")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Convert the WKT formatted geometry to Mosaic's internal format

# COMMAND ----------

# SOLUTION
df = df.withColumn("geom", mos.st_geomfromwkt("geom_wkt"))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert between formats
# MAGIC 
# MAGIC What is the binary representation of this geometry?

# COMMAND ----------

# SOLUTION
df = df.withColumn("geom_binary", mos.st_aswkb("geom"))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualise your HQ
# MAGIC 
# MAGIC Use the built-in kepler.gl support to visualise your company HQ.

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df "geom" "geometry"
