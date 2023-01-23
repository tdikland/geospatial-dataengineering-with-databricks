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

# MAGIC %md
# MAGIC ### Polygon packing
# MAGIC To _pack_ the polygon with cells we select all grid cells at the given resolution that are completely contained within the polygon. This gives us an _underestimation_ of the polygon in grid space. This may be useful when asking questions like "which points are _surely within_ a polygon". Note that choosing a resolution that is too high or an unfortunate location of the polygon can cause the packing to be empty (i.e. there exists no grid cell at a given resolution that is contained by the polygon)
# MAGIC 
# MAGIC ![Packing of Databricks HQ](https://tdikland.github.io/geospatial-dataengineering-with-databricks/resources/module3/packing.png)
# MAGIC 
# MAGIC _The packing of Databricks HQ at resolution 13_

# COMMAND ----------

# MAGIC %md
# MAGIC ### Polygon covering
# MAGIC To _cover_ the polygon with cells we select all grid cells at the given resolution that intersect with the polygon. This gives us an _overestimation_ of the polygon in grid space. This may be useful when answering questions like "find all points that _may be contained_ by the polygon". Note that the covering of a polygon is never empty.
# MAGIC 
# MAGIC ![H3 covering of Databricks HQ](https://tdikland.github.io/geospatial-dataengineering-with-databricks/resources/module3/cover.png)
# MAGIC 
# MAGIC _The covering of Databricks HQ at resolution 13_

# COMMAND ----------

# MAGIC %md
# MAGIC ### Polygon polyfill
# MAGIC To _polyfill_ the polygon with cells we select all grid cells at the given resolution whose center point is with the polygon. This gives us an _approximation_ of the polygon in grid space.
# MAGIC 
# MAGIC ![H3 polyfill of Databricks HQ](https://tdikland.github.io/geospatial-dataengineering-with-databricks/resources/module3/polyfill.png)
# MAGIC 
# MAGIC _The polyfill of Databricks HQ at resolution 13_
# MAGIC 
# MAGIC The polyfill strikes a balance between overestimating and underestimating the polygon. This is the method that is often used to approximate polygons the in H3 grid. Note that increasing the resolution yields better and better approximations of the polygon (why?), but that is traded off against the number of cells that make up said approximation. 
# MAGIC 
# MAGIC Another property of polyfill is that for multiple non-overlapping polygons, each H3 cell will only fall in one of the polygons. This avoid data duplications when querying segmentated datasets like countries.
# MAGIC 
# MAGIC The polyfill method is also included in the set of H3 geospatial functions. Let's inspect them more in depth.

# COMMAND ----------

# GeoJSON representation of the polygon describing the location Databricks HQ.
resolution = 13
hq_polygon = {
    "type": "Polygon",
    "coordinates": [
        [
            [-122.39373034022702, 37.79155217229324],
            [-122.39404669586622, 37.791303346996045],
            [-122.39377044164605, 37.79108855844848],
            [-122.39346002695783, 37.79132916852886],
            [-122.39373034022702, 37.79155217229324],
        ]
    ],
}

# create dataframe and polyfill the polygon
schema = "geom_geojson STRING"
df = spark.createDataFrame([{"geom_geojson": json.dumps(hq_polygon)}], schema)
df_polyfilled = df.withColumn(
    "polyfill", h3_polyfillash3(F.col("geom_geojson"), F.lit(resolution))
)

# does the result look like you expected? What would change if the resolution lowered?
display(df_polyfilled)
