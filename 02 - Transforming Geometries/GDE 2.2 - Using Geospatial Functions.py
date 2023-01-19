# Databricks notebook source
# MAGIC %md
# MAGIC # Using Geospatial Functions
# MAGIC 
# MAGIC Geospatial functions are used to work with geometries, e.g. measuring, transforming and relating geometries. Think about finding the area of a polygon, whether two lines are crossing or finding the smallest rectangle that encapsulates a polygon.
# MAGIC 
# MAGIC ## Learning objectives
# MAGIC By the end of this notebook, you should be able to:
# MAGIC - Apply geospatial functions to geometries with mosaic
# MAGIC - Understand why and how to reproject geometries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Run the setup script. This will import some needed modules and install the mosaic library. The mosaic library and its installation will be explained in depth in module 2.1

# COMMAND ----------

# MAGIC %run ../Includes/Setup-2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Measuring the area of a polygon
# MAGIC 
# MAGIC A common geometrical measurement is to find out the area of a polygon. Let's try to find the area of the foundation of Databricks HQ in San Fransisco.

# COMMAND ----------

# WKT representation of Databricks HQ in San Fransisco
geom = "POLYGON ((-122.39401298500093 37.79129663716428, -122.39401298500093 37.79129663716428, -122.39373988779624 37.79107781442336, -122.39343063138028 37.79132521190732, -122.39370182546828 37.79153952212819, -122.39401298500093 37.79129663716428))"

# Create DataFrame with geometry column
schema = "geom_wkt STRING"
df = spark.createDataFrame([{"geom_wkt": geom}], schema)
df_hq = df.withColumn("geom", mos.st_geomfromwkt(F.col("geom_wkt")))

# Calculate the area using the ST_AREA function
df_with_area = df_hq.withColumn("area", mos.st_area(F.col("geom")))

# Inspect the resulting area. Does it make sense? 
# What is the unit of area in this case?
display(df_with_area)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reprojecting geometries
# MAGIC 
# MAGIC When ingesting geometries in WKT format using mosaic, we have not specified a spatial reference. By default mosaic assumes that the coordinates represent degrees (ESPG:4326). Some questions can be better answered when the geometries are represented in a different projection. For example when we want to calculate the area of polygons it is wise to reproject to a coordinate system that uses meter as unit and is (approximately) equal-area. In the example question of finding the area of Databricks HQ, we will transform the geometries from ESPG:4326 to ESPG:32610 (UTM zone 10N).

# COMMAND ----------

# Reproject geometry 
df_hq = df_hq.withColumnRenamed("geom", "geom_espg_4326")
df_hq = df_hq.withColumn("geom_espg_32610", mos.st_transform(F.col("geom_espg_4326"), F.lit(32610)))

# Calculate area using the reprojected geometry
df_with_area = df_hq.withColumn("area_reprojected", mos.st_area(F.col("geom_espg_32610")))

# Inspect the resulting area
display(df_with_area)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Relating geometries
# MAGIC 
# MAGIC Apart from measuring geometries, we also want to know relations between geometries and create new geometries from these relations. The are many of such relations, for example:
# MAGIC - The _difference_ between two polygons (Find the part of polygon `A` that is not contained in polygon `B`)
# MAGIC - The _intersection_ between two polygons (Find the part of polygon `A` that is also part of polygon `B`)
# MAGIC - The _union_ of two polygons (The polygon whose interior is either in polygon `A` or polygon `B`)
# MAGIC - Two linestrings _crossing_ eachother
# MAGIC - A point _contained_ by a polygon
# MAGIC 
# MAGIC These relation can be expressed using e.g. `ST_INTERSECTION`, `ST_UNION` or `ST_CONTAINS`. A very common operation is to find which points are contained by polygon. Assume you have a dataset of cell phone pings of Databricks founders (points) and a set of Databricks office locations (polygons). Let's try to find out which executives have been to which offices. 

# COMMAND ----------

wkt1 = "LINESTRING (-1 -1, 1 1)"
wkt2 = "LINESTRING (-1 0, 1 0)"

df_left = spark.createDataFrame([(1, wkt1)], "id INT, wkt STRING").withColumn("geom_left", mos.st_geomfromwkt(F.col("wkt")))
df_right = spark.createDataFrame([(1, wkt2)], "id INT, wkt STRING").withColumn("geom_right", mos.st_geomfromwkt(F.col("wkt")))

df_res = df_left.join(df_right, "id").withColumn("crosses", mos.st_intersection(F.col("geom_left"), F.col("geom_right")))
display(df_res)

# COMMAND ----------


