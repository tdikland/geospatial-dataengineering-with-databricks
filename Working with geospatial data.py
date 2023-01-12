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
# MAGIC A set of coordinates alone is not enough to reconstruct the geospatial features. There also needs to be some information about the topology of the objects using conventions or explicitly. Say, for instance, you had the following set of coordinates: `(0,0), (1,0), (1,1), (0,1), (0,0)`. Do you have a line in the shape of a square? Do you have a square area? Do you have the area of the full map except the square? To solve this problem, stored geospatial vectors typically also contain information about the geometry and may follow conventions like the `right-hand-rule` (which states that the area bounded by a ring is on the right side of the boundary). More on that later in this notebook. First we have to know what kind of vector data objects there are.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Vector data objects
# MAGIC 
# MAGIC Coordinates for geometries may be 2, 3 or 4-dimensional. For simplicity we'll stick to 2D objects in this notebook. Let's first find the primitive geometry types in 2D.
# MAGIC 
# MAGIC ### Point
# MAGIC 
# MAGIC The basic representation of 0-dimensional object is a point. Think about a small tree, a cell phone location ping, or the top of a mountain.
# MAGIC 
# MAGIC ![point](files/geospatial/workshop/point.png)
# MAGIC 
# MAGIC _Representation of a point_
# MAGIC 
# MAGIC ### LineString
# MAGIC 
# MAGIC The linestring represents 1-dimensional objects. This can be used to represent roads, rivers and paths.
# MAGIC 
# MAGIC ![linestring](files/geospatial/workshop/linestring.png)
# MAGIC 
# MAGIC _Representation of a linestring_
# MAGIC 
# MAGIC ### Polygon
# MAGIC 
# MAGIC A polygon is used to represent 2-dimensional regions of interest. This can be a building, a country or a forrest for example. Note that polygons can also contain holes, for instance is you want a polygon that represents the landmass of a country you can do so by creating holes for lakes etc.
# MAGIC 
# MAGIC ![polygon](files/geospatial/workshop/polygon.png)
# MAGIC ![polygon with hole](files/geospatial/workshop/polygon_with_hole.png)
# MAGIC 
# MAGIC _Left: a polygon described by an external ring of points. Right: a polygon decribed by exterior and interior rings. the interior rings represent "holes" in the geometry._
# MAGIC 
# MAGIC ### MultiPoint, MultiLineString, MultiPolygon
# MAGIC 
# MAGIC Some objects are described by a set of homogeneous geometries. i.e. a country with multiple islands cannot be captured using only 1 polygon. A homogenuous collection of polygons is called a MultiPolygon. The definitions for MultiPoint and MultiLineString are analogous.
# MAGIC 
# MAGIC ![multipoint](files/geospatial/workshop/multipoint.png)
# MAGIC ![multilinestring](files/geospatial/workshop/multilinestring.png)
# MAGIC ![multipolygon](files/geospatial/workshop/multipolygon.png)
# MAGIC 
# MAGIC _From left to right: an example of a MultiPoint, MultiLineString and MultiPolygon._
# MAGIC 
# MAGIC ### GeometryCollection
# MAGIC 
# MAGIC A hetrogeneous collection of all of the above geometries is called a geometry collection.
# MAGIC 
# MAGIC ![geometry collection](files/geospatial/workshop/geometry_collection.png)
# MAGIC 
# MAGIC _An example of a geometry collection._

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geospatial data formats
# MAGIC 
# MAGIC Now that we know what the basic building blocks are in vector geometry, it is important to talk about data formats. How can we encode these geometries in a way that we can store them on disk, how can these geometries be fed into spark or other processing engines to do analysis, How can I transfer geometry objects and interpret them? This is what these data formats are for.
# MAGIC 
# MAGIC ### WKT/WKB (well-known text/binary representation of geometry)
# MAGIC 
# MAGIC WKT is a markup language for representing vector geometry objects. WKT is a human interpretable format and is often used in geospatial examples. The binary equivalent, WKB, is used to store and transfer the same information and is more compact and better machine readable.
# MAGIC 
# MAGIC A _point_ in WKT is specified by the string `POINT` followed by the coordinates. For example `POINT (10 40)`.
# MAGIC A _linestring_ in WKT is specified by the string `LINESTRING` followed by an ordered set of coordinates, comma seperated. For example `LINESTRING (20 10, 30 40, 40 50)`
# MAGIC A _polygon_ in WKT is specified by the string `POLYGON` and ...

# COMMAND ----------

# MAGIC %md
# MAGIC ### GeoJson
# MAGIC 
# MAGIC ...describe geojson...

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Setup mosaic
# MAGIC 
# MAGIC Talk about the prerequisites to enable mosaic.

# COMMAND ----------

# MAGIC %pip install databricks-mosaic

# COMMAND ----------

from mosaic import enable_mosaic

# spark.conf.set("mos")

enable_mosaic(spark, dbutils)

from mosaic import functions as mos
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## loading geometries with mosaic
# MAGIC 
# MAGIC Talk about mosaic geometry constructors

# COMMAND ----------

# Code example

geometries = [
    "POINT (0 1)",
    "LINESTRING (1 1, 3 5, 7 8)",
    "POLYGON ((10 10, 10 20, 20 20, 20 10, 10 10))",
    # "POLYGON ((10 10, 10 20, 20 20, 10 20, 10 10))"
    "POLYGON ((40 40, 40 50, 50 50, 50 40, 40 40), (42 42, 42 48, 48 48, 48 42, 42 42))"
]

schema = "geom_wkt STRING"
df = spark.createDataFrame([{"geom_wkt": g} for g in geometries], schema)

display(df)

# COMMAND ----------

import mosaic as mos

df_geoms = df.withColumn("geom", mos.st_geomfromwkt(F.col("geom_wkt")))

display(df_geoms)

# COMMAND ----------

# MAGIC %md
# MAGIC ### converting between formats/representations
# MAGIC 
# MAGIC TODO

# COMMAND ----------

## examples converting.

# >> show converting

# COMMAND ----------

## exercise!

## give some binary string, ask them which geometry is in there and what country the geometry belongs to?

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Visualising geometries using Kepler
# MAGIC 
# MAGIC Before we go futher into processing geospatial data, lets take some time to talk about geometry visualisation in the notebook. It is important to f
# MAGIC 
# MAGIC talk about usefulness of visualising from the notebook.
# MAGIC notebook magic command. 

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_geoms "geom" "geometry"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geometry functions
# MAGIC 
# MAGIC In order to work with the geometries in spark, the mosaic library exposes a set of functions that can measure, transform and relate geometries. In this section we dive into the usage of these functions to aid our analytical needs.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Measuring the area of a polygon
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
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reprojecting geometries
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

# EXERCISE: Find the perimeter of your companies HQ

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
# MAGIC ### Relating geometries
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

# Office locations
geom_databricks_hq = "POLYGON ((-122.39401298500093 37.79129663716428, -122.39401298500093 37.79129663716428, -122.39373988779624 37.79107781442336, -122.39343063138028 37.79132521190732, -122.39370182546828 37.79153952212819, -122.39401298500093 37.79129663716428))"

geom_databricks_office = "POLYGON ((-122.06188699366163 37.38742582735452, -122.06198446313843 37.38724441682429, -122.06179887057283 37.3868975077459, -122.06142501504554 37.386774444812204, -122.06133689195676 37.38694736939405, -122.06151046773734 37.387293217360735, -122.06188699366163 37.38742582735452))"

# Founder pings
ali = ["POINT (-122.3940 37.7911)", "POINT (-122.3937 37.79131)", "POINT (-122.3950 37.7911)"]
matei = ["POINT (-122.3937 37.79131)", "POINT (-122.3940 37.7911)", "POINT (-122.0616 37.3870)"]
reynold = ["POINT (-122.3940 37.7911)", "POINT (-122.3940 37.7911)", "POINT (-122.3940 37.7911)"]

pings = []
for founder, points in zip(["ali", "matei", "reynold"], [ali, matei, reynold]):
    for point in points:
        pings.append((founder, point))

# Prepare office locations
df_offices = spark.createDataFrame([("HQ", geom_databricks_hq), ("Silicon Valley", geom_databricks_office)], "location STRING, office_geom STRING")
df_offices = df_offices.withColumn("geom", mos.st_geomfromwkt(F.col("office_geom")))

# Prepare ping locations
df_founder_pings = spark.createDataFrame(pings, "founder STRING, ping STRING")
df_founder_pings = df_founder_pings.withColumn("geom", mos.st_geomfromwkt(F.col("ping")))

# Combine pings with offices
df_founders_in_offices = (
    df_offices
    .crossJoin(df_founder_pings)
    .where(mos.st_contains(df_offices.geom, df_founder_pings.geom))
    .select("founder", "location")
)

display(df_founders_in_offices)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Point-in-polygon join
# MAGIC 
# MAGIC What we just did is a so called _point-in-polygon join_. This is a very common operation in spatial data processing and one that is hard to scale to large datasets. Luckily the mosaic library has some interesting functionality that enhances the scalability of the point-in-polygon without compromising on precision. It does so using so called _discrete (global) grid systems_. If this sounds like abracadabra for now, no worries. We'll spend some time later on looking at this in depth.

# COMMAND ----------

# EXERCISE: 

df = spark.createDataFrame([{"geom_wkt": "POINT (0 0)"}])
df = df.withColumn("geom", mos.st_geomfromwkt(F.col("geom_wkt")))
df = df.withColumn("buffer", mos.st_buffer(F.col("geom"), F.lit(1.0)))

display(df)


# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df "buffer" "geometry"

# COMMAND ----------


