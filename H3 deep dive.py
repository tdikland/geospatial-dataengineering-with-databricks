# Databricks notebook source
## SETUP

## PREREQ KNOWLEDGE
# basic Python
# basic (Py)Spark

## DBR
# Tested with: DBR 12.0 + Photon
# Minimally needed: DBR 11.2 + Photon

## IMPORTS
from pyspark.sql import functions as F
from pyspark.databricks.sql.functions import *

# COMMAND ----------

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

# This cell requires DBR >= 11.3 + Photon
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

# COMMAND ----------


