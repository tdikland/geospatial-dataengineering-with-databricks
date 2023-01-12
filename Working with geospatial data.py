# Databricks notebook source
# MAGIC %md
# MAGIC # Geospatial vector data
# MAGIC 
# MAGIC Geospatial vector data is about points, lines and polygons and combinations thereof which can be represented by a finite number of coordinates.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Vector data
# MAGIC Vector data provides a way to represent real world features within the geospatial setting. A feature is anything you can see from some perspective. Think about trees, houses, neighborhoods, rivers, etc. Each one of these things would be a feature when we represent them in a geospatial application. In order to move from the real world setting geospatial vector data all features are represented with a set of coordinates.
# MAGIC 
# MAGIC ![real world](files/geospatial/workshop/real_world.png)
# MAGIC ![real world vectorized](files/geospatial/workshop/real_world_vectorized.png)
# MAGIC ![vectors with coordinates](files/geospatial/workshop/vectors_with_coordinates.png)
# MAGIC 
# MAGIC _Left: A representation of the real world. The green area could represent a forrest area. The red line can represent a curvy road trough the terrain and the blue point might be a water well. Middle: A finite number of coordinates is chosen to represent the boundaries of the features. Right: The coordinate values are calculated using a coordinate system._
# MAGIC 
# MAGIC A set of coordinates alone is not enough to reconstruct the geospatial features. There also needs to be some information about the topology of the objects using conventions or explicitly. Say, for instance, you had the following set of coordinates: `(0,0), (1,0), (1,1), (0,1), (0,0)`. Do you have a line in the shape of a square? Do you have a square area? Do you have the area of the full map except the square? To solve this problem, stored geospatial vectors typically also contain information about the geometry and may follow conventions like the `right-hand-rule` (which states that the area bounded by a ring is on the right side of the boundary).

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
# MAGIC ### WKT (well-known text)

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


