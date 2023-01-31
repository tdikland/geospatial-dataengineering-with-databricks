# Databricks notebook source
# MAGIC %md
# MAGIC # Raster Data Formats
# MAGIC 
# MAGIC This notebook explores geospatial raster data. The conversion from geospatial on earth is described first. Then we'll discuss some common raster data formats.
# MAGIC 
# MAGIC ## Learning Goals:
# MAGIC - Describe the raster data format
# MAGIC - Know the differences between raster and vector data
# MAGIC - Describe some strengths and weaknesses of the raster data format

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is raster data?
# MAGIC 
# MAGIC Raster data is a grid of values which are rendered on a map as pixels. Each of those pixels represent an area on the Earth's surface. The value of a pixel can represent continuous (e.g. elevation) or categorical (e.g. land use) data. This is in contrast with vector data which only represent specific features and their attributes on Earth's surface.
# MAGIC 
# MAGIC While this definition may sound a bit dry, you already have some experience working with rasters! The standard digital photo is a raster where each pixel represents the color value in red, green and blue (RGB). What makes a raster a _geospatial_ raster is the addition of spatial information that connects the image to a specific location on the Earth's surface. This spatial information includes the rasters extend, cell size, number of rows and columns and its coordinate reference system.
# MAGIC 
# MAGIC ![raster data format](https://tdikland.github.io/geospatial-dataengineering-with-databricks/resources/module6/raster_format.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Examples of raster data
# MAGIC 
# MAGIC Some examples of continuous rasters include:
# MAGIC - Precipitation maps.
# MAGIC - Maps of tree height derived from LiDAR data.
# MAGIC - Elevation values for a region.
# MAGIC 
# MAGIC Some examples of classified maps include:
# MAGIC - Landcover / land-use maps.
# MAGIC - Tree height maps classified as short, medium, and tall trees.
# MAGIC - Elevation maps classified as low, medium, and high elevation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raster data (dis)advantages
# MAGIC 
# MAGIC Raster data has some important advantages:
# MAGIC - representation of continuous surfaces
# MAGIC - potentially very high levels of detail
# MAGIC - data is ‘unweighted’ across its extent - the geometry doesn’t implicitly highlight features
# MAGIC - cell-by-cell calculations can be very fast and efficient
# MAGIC 
# MAGIC The downsides of raster data are:
# MAGIC - very large file sizes as cell size gets smaller
# MAGIC - currently popular formats don’t embed metadata well
# MAGIC - can be difficult to represent complex information

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading GeoTIFF with rasterio

# COMMAND ----------

# MAGIC %pip install rasterio

# COMMAND ----------

import rasterio
import rasterio.plot
# import pyproj
import numpy as np
import matplotlib
import matplotlib.pyplot as plt

# COMMAND ----------

print('Landsat on Google:')
filepath = 'https://storage.googleapis.com/gcp-public-data-landsat/LC08/01/042/034/LC08_L1TP_042034_20170616_20170629_01_T1/LC08_L1TP_042034_20170616_20170629_01_T1_B4.TIF'
with rasterio.open(filepath) as src:
    print(src.profile)

# COMMAND ----------

# The grid of raster values can be accessed as a numpy array and plotted:
with rasterio.open(filepath) as src:
    thumbnail = src.read(1, out_shape=(1, int(src.height), int(src.width)))

plt.imshow(thumbnail)
plt.colorbar()
plt.title("Overview - Band 4 {}".format(thumbnail.shape))
plt.xlabel("Column #")
plt.ylabel("Row #")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading GeoTIFF with mosaic

# COMMAND ----------

# MAGIC %pip install databricks-mosaic

# COMMAND ----------

import mosaic as mos
from pyspark.sql import functions as F
mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

# COMMAND ----------

# sample tiff file
df_raw = spark.read.format("binaryFile").load("dbfs:/FileStore/geospatial/samples/bogota.tif")
display(df_raw)
