# Databricks notebook source
# MAGIC %md 
# MAGIC # Transforming Geometries Lab
# MAGIC This notebook provides a hands-on review focused on setting up mosaic, transforming and measuring geometries
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab you should be able to
# MAGIC - Install, configure and enable mosaic in the Databricks notebook
# MAGIC - Reproject geometries depending on use case
# MAGIC - Measure geometries using mosaic functions
# MAGIC - Relate geometries using mosaic functions 

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Setting up mosaic

# COMMAND ----------

# MAGIC %md
# MAGIC Install the mosaic library with pip

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md
# MAGIC configure mosaic to use `JTS` and `H3` as geometry api and index system

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md
# MAGIC enable the mosaic functions

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md
# MAGIC # Transforming geometries
# MAGIC 
# MAGIC Can you find the perimeter of your companies HQ?

# COMMAND ----------

# Find the WKT representation using e.g. https://geojson.io
geom = "TODO"

schema = "geom_wkt STRING"
df = spark.createDataFrame([{"geom_wkt": geom}], schema)
df_hq = df.withColumn("geom", mos.st_geomfromwkt(F.col("geom_wkt")))

# Reproject your geometry if necessary
df_hq = df_hq.withColumn("geom_reprojected", mos.st_transform(F.col("TODO"), F.lit("TODO")))

# Calculate the perimeter
df_with_area = df_hq.withColumn("perimeter", "TODO")

display(df_with_area)

# COMMAND ----------

# MAGIC %md
# MAGIC Can you calculate the area of three office locations, without calculating the area of these offices independently?

# COMMAND ----------

# TODO
