# Databricks notebook source
# MAGIC %md
# MAGIC # Geospatial vector data
# MAGIC 
# MAGIC Geospatial vector data is about points, lines and polygons and combinations thereof which can be represented by a finite number of coordinates.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Vector data formats
# MAGIC There are many ways to structure and store geospatial vector data in memory and on disk. Some popul
# MAGIC 
# MAGIC Talk about:
# MAGIC - GeoJson
# MAGIC - WKT/WKB

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Setup mosaic
# MAGIC 
# MAGIC Talk about the prerequisites to enable mosaic.

# COMMAND ----------

# MAGIC %pip install databricks-mosaic

# COMMAND ----------

# import dependencies
# use enable_mosaic

# COMMAND ----------

# MAGIC %md
# MAGIC ## loading geometries with mosaic
# MAGIC 
# MAGIC Talk about mosaic geometry constructors

# COMMAND ----------

# Code example

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Visualising geometries using Kepler
# MAGIC 
# MAGIC talk about usefulness of visualising from the notebook.
# MAGIC notebook magic command.

# COMMAND ----------

# %%mosaic_kepler ...

# COMMAND ----------


