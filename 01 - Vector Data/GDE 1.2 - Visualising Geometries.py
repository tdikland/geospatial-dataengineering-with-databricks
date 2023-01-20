# Databricks notebook source
# MAGIC %md
# MAGIC # Visualising Geometries
# MAGIC 
# MAGIC Often geometries are fairly complex, and it is hard or even impossible to obtain useful information from a textual representation. This is where visualisation of geometries helps to quickly understand the shape or position of some geometry.
# MAGIC 
# MAGIC ## Learning objectives
# MAGIC By the end of this notebook you should be able to:
# MAGIC - Understand how the `%%mosaic_kepler` magic command works
# MAGIC - Visualise WKT/WKB or Mosaic internel geometries
# MAGIC - Navigate the Kepler visualisation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Run the setup script. This will import some needed modules and install the mosaic library. The mosaic library and its installation will be explained in depth in module 2.1

# COMMAND ----------

# MAGIC %run ../Includes/Setup-1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geometry visualisation with kepler.gl
# MAGIC Mosaic registers a magic function in the notebook to easily produce visualisations using [kepler.gl](https://kepler.gl/). 
# MAGIC 
# MAGIC For a visualisation we need 4 parameters:
# MAGIC - _dataframe_: the dataframe that contains the column with geometries that need to be visualised
# MAGIC - _column-name_: the name of the column in the aforementioned dataframe, which contains the geometries. The geometries must be in `WKT`, `WKB` or `Mosiac Internal` format
# MAGIC - _feature-type_: the type of spatial feature to be visualised. In this case this is always equal to the string `geometry`
# MAGIC - _limit_: the amount of geometries to plot. This is an optional parameter. When it is not provided it plots up to 1000 geometries by default
# MAGIC 
# MAGIC The kepler magic command can be used as follows:
# MAGIC 
# MAGIC ```
# MAGIC %%mosaic_kepler
# MAGIC dataframe column-name feature-type [<limit>]
# MAGIC ```
# MAGIC 
# MAGIC Let's see that in action!

# COMMAND ----------

# Create some geometries to visualise
databricks_hq_geojson = '{"type": "Polygon", "coordinates": [[[-122.39343385977625, 37.79132004377702], [-122.39370019061133, 37.791537245784454], [-122.39400700373288, 37.79128973646942], [-122.39373641160483, 37.79107926871191], [-122.39343385977625, 37.79132004377702]]]}'
san_fransisco_road = '{"coordinates": [[-122.39492103462949, 37.79181899994789], [-122.39340774317989, 37.790613052399536], [-122.39251569769378, 37.791302887017565], [-122.3940767772944, 37.79254658717453]], "type": "LineString"}'
bus_stop = '{"coordinates": [-122.39315228821998, 37.791682551863005], "type": "Point"}'

df = spark.createDataFrame([(databricks_hq_geojson,), (san_fransisco_road,), (bus_stop,)], "geom_geojson STRING")

# Note that the %%mosaic_kepler command cannot visualise geojson! We have to convert it into Mosaic's internal format
df_viz = df.withColumn("geom", mos.st_geomfromgeojson(F.col("geom_geojson")))

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_viz "geom" "geometry" 3
