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
# MAGIC 
# MAGIC ![UTM zone 10](https://tdikland.github.io/geospatial-dataengineering-with-databricks/resources/module2/utm_projection.png)

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
# MAGIC ## Accessing geometry attributes
# MAGIC 
# MAGIC The mosaic library contains a range of functions that measure or access properties of geometries. An example was already observed above when we tried to measure the area of a geometry. Some examples:
# MAGIC - ST_XMAX, to find the maximal first coordinate of a geometry
# MAGIC - ST_NUMPOINTS, to find the amount of points in a geometry
# MAGIC - ST_SCALE, to strech a geometry

# COMMAND ----------

geom = "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"

schema = "geom_wkt STRING"
df = spark.createDataFrame([{"geom_wkt": geom}], schema)
df_geom = df.withColumn("geom", mos.st_geomfromwkt(F.col("geom_wkt")))

# apply the functions to the square geometry
df_attrs = (df_geom
    .withColumn("xmax", mos.st_xmax(F.col("geom")))
    .withColumn("points", mos.st_numpoints(F.col("geom")))
    .withColumn("scaled", mos.st_scale(F.col("geom"), F.lit(0.5), F.lit(2)))
    .withColumn("scaled_wkt", mos.st_aswkt(F.col("scaled")))
)

# inspect the results; what did you expect to see?
display(df_attrs)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Geometric perdicates
# MAGIC 
# MAGIC There is also a range of functions aimed at testing whether some property holds for a (combination of) geomertries. Some examples:
# MAGIC - ST_CONTAINS, tests whether the first geometry is contained in the second geometry
# MAGIC - ST_INTERSECTS, tests whether two geometries intersect
# MAGIC - ST_ISVALID, tests whether a geometry is valid

# COMMAND ----------

geom_one = "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"
geom_two = "LINESTRING(-2 -2, 2 2)"
geom_three = "POINT (0.5 0.5)"

schema = "geom1_wkt STRING, geom2_wkt STRING, geom3_wkt STRING"
df = spark.createDataFrame([{"geom1_wkt": geom_one, "geom2_wkt": geom_two, "geom3_wkt": geom_three}], schema)
df_geom = (df
    .withColumn("geom1", mos.st_geomfromwkt(F.col("geom1_wkt")))
    .withColumn("geom2", mos.st_geomfromwkt(F.col("geom2_wkt")))
    .withColumn("geom3", mos.st_geomfromwkt(F.col("geom3_wkt")))
    .select("geom1", "geom2", "geom3")
)

# apply the functions to the square geometry
df_attrs = (df_geom
    .withColumn("contains", mos.st_contains(F.col("geom1"), F.col("geom3")))
    .withColumn("intersects", mos.st_intersects(F.col("geom1"), F.col("geom2")))
    .withColumn("valid", mos.st_isvalid(F.col("geom1")))
)

# inspect the results; what did you expect to see?
display(df_attrs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Relating geometries
# MAGIC 
# MAGIC Apart from measuring and checking geometries, we also want to know relations between geometries and create new geometries from these relations. Here too mosaic can help, for example:
# MAGIC - ST_DIFFERENCE, the difference between two polygons (Find the part of polygon `A` that is not contained in polygon `B`)
# MAGIC - ST_INTERSECTION, intersection between two polygons (Find the part of polygon `A` that is also part of polygon `B`)
# MAGIC - ST_UNION, the union of two polygons (The polygon whose interior is either in polygon `A` or polygon `B`)

# COMMAND ----------

wkt1 = "LINESTRING (-1 -1, 1 1)"
wkt2 = "LINESTRING (-1 0, 1 0)"

df_left = spark.createDataFrame([(1, wkt1)], "id INT, wkt STRING").withColumn("geom_left", mos.st_geomfromwkt(F.col("wkt")))
df_right = spark.createDataFrame([(1, wkt2)], "id INT, wkt STRING").withColumn("geom_right", mos.st_geomfromwkt(F.col("wkt")))

df_res = df_left.join(df_right, "id").withColumn("crosses", mos.st_intersects(F.col("geom_left"), F.col("geom_right")))
display(df_res)

# COMMAND ----------


