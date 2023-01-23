# Databricks notebook source
# MAGIC %md
# MAGIC # Indexing Points and Polygons
# MAGIC 
# MAGIC In this notebook we'll learn how to find the unique grid cell that a point belongs to given a grid resolution. This is effectively a way of indexing points in the grid system.
# MAGIC 
# MAGIC ## Learning goals
# MAGIC - Use DBR built-in H3 functionality to find H3 grid cells for point data
# MAGIC - Use DBR build-in H3 functionality to find H3 grid cells for polygon data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Run the setup script to make sure dependencies are in scope

# COMMAND ----------

# MAGIC %run ../Includes/Setup-3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Indexing points with H3
# MAGIC 
# MAGIC Databricks Runtime (DBR) has built-in functions to find the H3 cell id of a point, given a valid resolution. Two types of point data are supported, long/lat pairs and WKT/WKB/GeoJSON.
# MAGIC 
# MAGIC Before the H3 functions in DBR can be used, they must be imported.

# COMMAND ----------

from pyspark.databricks.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Indexing longitude/latitude pairs
# MAGIC 
# MAGIC Now that the functions are imported, the indexing can begin. The following functions are availeble for indexing long/lat pairs:
# MAGIC - `h3_longlatash3`
# MAGIC - `h3_longlatash3string`
# MAGIC 
# MAGIC The difference between the two is the representation of the H3 cell id. The first expression will yield a `BIGINT` and the second one yields a hexadecimal `STRING`. While both representations are correct, the first one will be much faster during query processing. If you visit the [documentation for H3 functions in Databricks](https://docs.databricks.com/sql/language-manual/sql-ref-h3-geospatial-functions.html) you'll see that most functions have these two flavours (`BIGINT/STRING`).

# COMMAND ----------

# specify long/lat pair of Databricks HQ
longitude = -122.39363
latitude = 37.79125

# pick the H3 resolution used for indexing
resolution = 7

schema = "long DOUBLE, lat DOUBLE"
df_location = spark.createDataFrame([{"long": longitude, "lat": latitude}], schema)

# find the bigint and string representation of the h3 cell in resolution 7
df_location_with_cell_id = (
    df_location
    .withColumn("h3_cell_id_int", h3_longlatash3(F.col("long"), F.col("lat"), F.lit(resolution)))
    .withColumn("h3_cell_id_string", h3_longlatash3string(F.col("long"), F.col("lat"), F.lit(resolution)))
)

# compare the bigint and string. Is this what you expected?
display(df_location_with_cell_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Indexing WKT/WKB/GeoJSON
# MAGIC 
# MAGIC In the previous section we have already discussed how to map longitude/latitude pairs to a H3 grid cell. Points represented using WKT, WKB or GeoJSON follow in essence the same approach. 

# COMMAND ----------

# specify the Databricks HQ point in WKT
point = "POINT (-122.39363 37.79125)" 

# pick the H3 resolution used for indexing
resolution = 7

schema = "geom_wkt STRING"
df_point = spark.createDataFrame([{"geom_wkt": point}], schema)

# find the bigint and string representation of the h3 cell in resolution 7
df_point_with_cell_id = (
    df_point
    .withColumn("h3_cell_id_int", h3_pointash3(F.col("geom_wkt"), F.lit(resolution)))
    .withColumn("h3_cell_id_string", h3_pointash3string(F.col("geom_wkt"), F.lit(resolution)))
)

# compare the bigint and string. Is this what you expected?
display(df_point_with_cell_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Indexing polygons with H3
# MAGIC Indexing polygons is a more interesting. There are several ways to represent a polygon geometry in the H3 grid. It turns out that the hexagonal tiling is the best way to approximate two dimensional shapes. This is of course no coincidence.
# MAGIC 
# MAGIC The ones we will look at are:
# MAGIC - grid cell packing
# MAGIC - grid cell covering
# MAGIC - grid cell polyfill

# COMMAND ----------


