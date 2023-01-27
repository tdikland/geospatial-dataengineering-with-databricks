# Databricks notebook source
# MAGIC %md
# MAGIC # Indexing Geometries with H3 Lab
# MAGIC 
# MAGIC this notebook provides a hands-on review of H3 indexing points and polygons as well as how to visualise them.
# MAGIC 
# MAGIC ## Learning objectives
# MAGIC By the end of this lab you should be able to:
# MAGIC - Index point data in the H3 grid system
# MAGIC - Index polygon data in the H3 grid system
# MAGIC - Visualise grid cells using kepler.gl

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Run the following cell to install mosaic for visualisation and the import of h3 functions.

# COMMAND ----------

# MAGIC %run ../Includes/Setup-3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Indexing points and polygons
# MAGIC Find the coordinates of the location you are right now (e.g. using Google Maps). What H3 cell does that belong to in resolution 7? And what about the other resolution?

# COMMAND ----------

# SOLUTION
# Leiden, The Netherlands
longitude = 4.505955
latitude = 52.159022

schema = "long DOUBLE, lat DOUBLE"
df_point = spark.createDataFrame([{"long": longitude, "lat": latitude}], schema)
df_resolutions = spark.range(16).withColumnRenamed("id", "resolution")

df_h3_cell = (
    df_point.crossJoin(df_resolutions)
    .withColumn("cell_id", h3_longlatash3("long", "lat", df_resolutions.resolution))
    .orderBy(F.col("resolution"))
)

# calculate the h3 cell in bigint and string. Does something stand out?
display(df_h3_cell)

# COMMAND ----------

# MAGIC %md
# MAGIC visualise all H3 cells your current location is part of. 

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_h3_cell "cell_id" "h3"

# COMMAND ----------

# MAGIC %md
# MAGIC Find the geojson representation of the polygon describing the location of your companies HQ and calculate the polyfill.
# MAGIC 
# MAGIC TIP: use https://geojson.io to draw a polygon and find its geojson representation.

# COMMAND ----------

# SOLUTION
resolution = 12
hq_polygon = "POLYGON ((4.888677027629569 52.33591764757733,4.888677027629569 52.335759226198405,4.889275339651931 52.335759226198405,4.889275339651931 52.33591764757733,4.888677027629569 52.33591764757733))"


schema = "geom_wkt STRING"
df = spark.createDataFrame([{"geom_wkt": hq_polygon}], schema)
df_polyfilled = df.withColumn("polyfill", F.explode(h3_polyfillash3("geom_wkt", F.lit(resolution))))

display(df_polyfilled)

# COMMAND ----------

# MAGIC %md
# MAGIC visualise the grid cells that are part of the polyfill above

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_polyfilled "polyfill" "h3"
