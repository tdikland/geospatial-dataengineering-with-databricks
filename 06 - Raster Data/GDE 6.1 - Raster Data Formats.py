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
