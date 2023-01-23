# Databricks notebook source
# MAGIC %md
# MAGIC # Visualising H3 cells
# MAGIC 
# MAGIC The Databricks Labs project mosaic has functionality for rendering H3 indices in Databricks notebooks using Kepler.gl. In this notebook it is shown how the magic command can be used to do that.
# MAGIC 
# MAGIC ## Learning Goals:
# MAGIC - Use `mosaic_kepler` magic command to visualise H3 cells

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Run the setup script to install mosaic and H3 functions

# COMMAND ----------

# MAGIC %run ../Includes/Setup-3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geometry visualisation with kepler.gl
# MAGIC Mosaic registers a magic function in the notebook to easily produce visualisations using [kepler.gl](https://kepler.gl/). 
# MAGIC 
# MAGIC For a visualisation we need 4 parameters:
# MAGIC - _dataframe_: the dataframe that contains the column with cell ids that need to be visualised
# MAGIC - _column-name_: the name of the column in the aforementioned dataframe, which contains the cell ids.
# MAGIC - _feature-type_: the type of spatial feature to be visualised. In this case this is always equal to the string `h3`
# MAGIC - _limit_: the amount of grid cells to plot. This is an optional parameter. When it is not provided it plots up to 1000 geometries by default
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
df_viz = df.withColumn(
    "grid_cells", F.explode(h3_polyfillash3(F.col("geom_geojson"), F.lit(resolution)))
).select("grid_cells")

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_viz "grid_cells" "h3" 300
