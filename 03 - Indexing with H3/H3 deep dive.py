# Databricks notebook source
# MAGIC %md 
# MAGIC # H3 deep dive
# MAGIC The H3 grid is a *discrete global grid system*. Let's start off by disecting what each of these words mean.
# MAGIC - *Discrete* means that the grid has a finite amount of cells. Contrast this with the amount of points on the earths surface is of infinite size.
# MAGIC - *Global* means that every point on the globe can be associated with a unique grid cell.
# MAGIC - *System* means that the grid is actually a series of dicrete global grids with progressively finer resolution.
# MAGIC 
# MAGIC Below a visualisation of the H3 grid is shown. Verify for yourself that this indeed satifies all the properties of a discrete global grid system.
# MAGIC 
# MAGIC ![discrete global grid system](files/geospatial/workshop/discrete_global_grid_system.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Longitude and latitude
# MAGIC Perhaps the simplest form of geospatial data is a longitude/latitude pair. These pairs are often used to annotate the point on the globe where an event happened. If you used a smart phone or laptop today, you've probably already (unconciously) sent a lot of these geotagged events to the internet. Data engineers, scientists and analysts that want to derive insights from these events need to figure out how to properly interpret these longitude/latitude pairs. The H3 grid can be used to do so. The first step often is to find out to which grid cell the longitude/latitude pair belongs.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.databricks.sql.functions import *

import json

# COMMAND ----------

# find your current longitude and latitude (e.g. using https://www.latlong.net/), and complete the cell below.

# TODO
longitude = -122.39363
latitude = 37.79125

# Ranges from 0 to 15 inclusive.
resolution = 7

schema = "long DOUBLE, lat DOUBLE"
df_location = spark.createDataFrame([{"long": longitude, "lat": latitude}], schema)
df_location_with_cell_id = (
    df_location
    .withColumn("h3_cell_id_int", h3_longlatash3(F.col("long"), F.col("lat"), F.lit(resolution)))
    .withColumn("h3_cell_id_string", h3_longlatash3string(F.col("long"), F.col("lat"), F.lit(resolution)))
)

display(df_location_with_cell_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### H3 index structure
# MAGIC As can be seen from the previous cell, it is possible to map every long/lat pair to a grid cell with the specified resolution. Let's discover roughly how these cell ids are structured. Let's pick some location on the globe and find it's H3 grid cell id for all possible resolutions. What do you notice?

# COMMAND ----------

# Location of Databricks HQ in San Fransisco.
longitude = -122.39363
latitude = 37.79125

schema = "long DOUBLE, lat DOUBLE"
df_location = spark.createDataFrame([{"long": longitude, "lat": latitude}], schema)
df_resolution = spark.range(16).withColumnRenamed("id", "resolution")
df = df_location.crossJoin(df_resolution)

df_location_with_cell_id = (
    df
    .withColumn("h3_cell_id_int", h3_longlatash3("long", "lat", "resolution"))
    .withColumn("h3_cell_id_string", h3_longlatash3string("long", "lat", "resolution"))
)

display(df_location_with_cell_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Databricks HQ in increasing resolution
# MAGIC Let's visualize the result of the previous cell. You can clearly see that location gets more and more precise when the resolution increases. Have you also noticed that the `h3_cell_id_string` went from ending in a lot of `f`s to not ending in any `f`s at all? The hexidecimal representation of the cell id(`h3_cell_id_string`) is structured such that the first bits are reserved for cell metadata, and then 3 bits for each resolution. This structure makes operations on cells very fast, e.g. you can quickly find the cell one resolution lower then a given grid cell (think about why!)
# MAGIC 
# MAGIC ![Databricks HQ H3 grid cell id zoom 0](files/geospatial/workshop/databricks_hq_h3_zoom_0.png)
# MAGIC ![Databricks HQ H3 grid cell id zoom 1](files/geospatial/workshop/databricks_hq_h3_zoom_1.png)
# MAGIC ![Databricks HQ H3 grid cell id zoom 2](files/geospatial/workshop/databricks_hq_h3_zoom_2.png)
# MAGIC ![Databricks HQ H3 grid cell id zoom 3](files/geospatial/workshop/databricks_hq_h3_zoom_3.png)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Traversing H3 resolutions
# MAGIC In the previous section we already hinted at the possibility of converting h3 cells to higher/lower resolution. It is important to note that H3 is not fully hierarchical (i.e. you cannot recursively partition cells to get to the next resolutions), since you cannot tile a hexagon with smaller hexagons (try it!). To "fix" this, the subdivision is rotated to make it fit approximately. Every odd numbered resolution has its tiles slightly rotated versus the even numbered resolutions. Therefore the H3 grid is known as a *pseudo-hierarchical* grid system.
# MAGIC 
# MAGIC ![Subdivision of H3 grid](files/geospatial/workshop/subdivision.png)
# MAGIC 
# MAGIC All the cells within one of the subdivisions of a given H3 cell are called its *children*. Vice verse, every lower resolution cell containing a given H3 cell is called its *parent*.

# COMMAND ----------

cell_id = "87283082affffff"
cell_resolution = 7

parent_resolution = 5
child_resolution = 9

schema = "h3_cell_id STRING"
df = spark.createDataFrame([{"h3_cell_id": cell_id}], schema)

df_traversed = (
    df
    .withColumn("parent_cell_id", h3_toparent(F.col("h3_cell_id"), F.lit(parent_resolution)))
    .withColumn("child_cell_ids", h3_tochildren(F.col("h3_cell_id"), F.lit(child_resolution)))
)

# How many child cells do you expect to find?
display(df_traversed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Higher dimensional geospatial vector data
# MAGIC H3 indexing is not only useful for longitude/latitude pairs. In this section we will discuss a broader category of geospatial data called vector data. Geospatial vector data is about *points*, *lines* and *polygons* and combinations thereof which can be represented by a finite number of coordinates.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Indexing points
# MAGIC In the previous section we have already discussed how to map longitude/latitude pairs to a H3 grid cell. Points follow in essence the same approach. 

# COMMAND ----------

# WKT representation of the point geometry
point = "POINT (-122.39363 37.79125)" 
resolution = 15

schema = "geom_wkt STRING"
df = spark.createDataFrame([{"geom_wkt": point}], schema)
df = df.withColumn("h3_grid_cell", h3_pointash3string(F.col("geom_wkt"), F.lit(resolution)))

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Indexing polygons
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
# MAGIC ![Packing of Databricks HQ](files/geospatial/workshop/packing.png)
# MAGIC 
# MAGIC _The packing of Databricks HQ at resolution 13_

# COMMAND ----------

# MAGIC %md
# MAGIC ### Polygon covering
# MAGIC To _cover_ the polygon with cells we select all grid cells at the given resolution that intersect with the polygon. This gives us an _overestimation_ of the polygon in grid space. This may be useful when answering questions like "find all points that _may be contained_ by the polygon". Note that the covering of a polygon is never empty.
# MAGIC 
# MAGIC ![H3 covering of Databricks HQ](files/geospatial/workshop/cover.png)
# MAGIC 
# MAGIC _The covering of Databricks HQ at resolution 13_

# COMMAND ----------

# MAGIC %md
# MAGIC ### Polygon polyfill
# MAGIC To _polyfill_ the polygon with cells we select all grid cells at the given resolution whose center point is with the polygon. This gives us an _approximation_ of the polygon in grid space.
# MAGIC 
# MAGIC ![H3 polyfill of Databricks HQ](files/geospatial/workshop/polyfill.png)
# MAGIC 
# MAGIC _The polyfill of Databricks HQ at resolution 13_
# MAGIC 
# MAGIC The polyfill strikes a balance between overestimating and underestimating the polygon. This is the method that is often used to approximate polygons the in H3 grid. Note that increasing the resolution yields better and better approximations of the polygon (why?), but that is traded off against the number of cells that make up said approximation. The polyfill method is also included in the set of H3 geospatial functions. Let's expect them more in depth.

# COMMAND ----------

# Find the geojson representation of the polygon describing the location of your companies HQ and calculate the polyfill.
# TIP: use https://geojson.io to draw a polygon and find its geojson representation.

# TODO
resolution = 13
hq_polygon = {
    "type": "Polygon",
    "coordinates": [
        [
            [4.888213771501597, 52.335756111437945],
            [4.889285938078785, 52.335756111437945],
            [4.889285938078785, 52.33589961572261],
            [4.888213771501597, 52.33589961572261],
            [4.888213771501597, 52.335756111437945],
        ]
    ],
}


schema = "geom_geojson STRING"
df = spark.createDataFrame([{"geom_geojson": json.dumps(hq_polygon)}], schema)
df = df.withColumn("polyfill", h3_polyfillash3string(F.col("geom_geojson"), F.lit(resolution)))

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grid distances
# MAGIC We have seen how basic geometries can be mapped to the H3 grid system. One of the properties of the H3 grid system is that the centers of all neighbouring cells lie at the same distance. 
# MAGIC 
# MAGIC ![Equidistant neighbour cells](files/geospatial/workshop/neighbours.png)
# MAGIC 
# MAGIC _For a hexagonal grid all neighour cells are equidistant. In square grids there are two possible distances to a neighbour cell and in a triangular grid even three._
# MAGIC 
# MAGIC The advantage is that if you take the set of cells that are within `k` cells of some origin cell, the resulting set will look like a "disk" around the origin cell. The "disk" is called the `kRing`. This is very useful when answering questions like "find the closest point of interest to a given point".

# COMMAND ----------

geom = "POINT (-122.39363 37.79125)" 
resolution = 13

schema = "geom_wkt STRING"
df = spark.createDataFrame([{"geom_wkt": geom}], schema)
df = (
    df
    .withColumn("cell_id", h3_pointash3string(F.col("geom_wkt"), F.lit(resolution)))
    .withColumn("1ring", h3_kring(F.col("cell_id"), F.lit(1)))
    .withColumn("2ring", h3_kring(F.col("cell_id"), F.lit(2)))
    .withColumn("3ring", h3_kring(F.col("cell_id"), F.lit(3)))
)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ![Databricks HQ - 1ring](files/geospatial/workshop/1ring.png)
# MAGIC ![Databricks HQ - 2ring](files/geospatial/workshop/2ring.png)
# MAGIC ![Databricks HQ - 3ring](files/geospatial/workshop/3ring.png)
# MAGIC 
# MAGIC _kRings around Databricks HQ with k=1,2,3_

# COMMAND ----------

# CHALLANGE: Find the point of interest closest to the origin point using H3 functions

import random
random.seed(42)

origin_point = "POINT (0, 0)"
points_of_interest = [f"POINT({round(random.uniform(-1.000, 1.000), 3)} {round(random.uniform(-1.000, 1.000), 3)})" for _ in range(10)]

schema = "geom_wkt STRING"
df_origin = spark.createDataFrame([{"geom_wkt": origin_point}], schema)
df_poi = spark.createDataFrame([{"geom_wkt": wkt} for wkt in points_of_interest], schema)

display(df_origin)
display(df_poi)

# TODO
# ...

# COMMAND ----------

# MAGIC %md
# MAGIC ## H3 map projection
# MAGIC 
# MAGIC For map projection, the gnomonic projections centered on icosahedron faces is used. This projects from Earth as a sphere to an icosahedron, a twenty-sided platonic solid.
# MAGIC 
# MAGIC ![H3 map projection](files/geospatial/workshop/map_projection.png)
# MAGIC 
# MAGIC _Left: The icosahedron, a solid made out of 20 regular triangles that touch in 12 vertices._
# MAGIC _Middle: The icosahedron faces are projected onto the sphere with a gnomonic projection around the center of the face._ 
# MAGIC _Right: The Earth approximated as a sphere along with the projection of the icosahedron. Note that all vertices are projected into the ocean._
# MAGIC 
# MAGIC ### Projection
# MAGIC This choice of projection has the following consequences:
# MAGIC - The H3 map projection is _not conformal_. Shapes will be distorted.
# MAGIC - The H3 map projection is _not equal-area_. Cells close to the edges of the icosahedron centers will have up to 1.6x more area as cells that are close to the icosahedron edges.
# MAGIC - In the gnomonic projection straight lines are mapped to _geodesics_. This means that the shortest path between two cells in the H3 grid is also the shortest path on earth (something that is _not_ true for the well-known Mercator projection)
# MAGIC 
# MAGIC ### Pentagons
# MAGIC It is also no coincidence that all vertices of the icosahedron are mapped into the ocean. In order to make the H3 grid fit on the globe, 12 pentagons were added (because... well, mathematics) and these pentagons are located at the vertices of the icosahedron. Since they are all mapped into the ocean, we typically don't have to deal with them.
