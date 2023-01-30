# Databricks notebook source
# MAGIC %md
# MAGIC # Hybrid Point in Polygon Joins
# MAGIC 
# MAGIC This notebook shows how to do point-in-polygon joins with tessellated polygons.
# MAGIC 
# MAGIC ## Learning goals
# MAGIC - How to select a suitable grid for polygon tessellation
# MAGIC - How to select a good resolution for polygon tessellation
# MAGIC - Running different types of hybrid PIP joins

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Run the following cell to setup the mosaic library

# COMMAND ----------

# MAGIC %run ../Includes/Setup-4

# COMMAND ----------

# MAGIC %md
# MAGIC ## Choosing the tessellation configurations
# MAGIC 
# MAGIC In the previous notebook we quickly looked at how to tessellate a polygon with mosaic. However there are some key configurations to set before.
# MAGIC - What is the best grid system to use for tessellation?
# MAGIC - What is the best resolution to use for tessellation?
# MAGIC 
# MAGIC The grid system depends on your use case! If your other geometries (e.g. points) are already indexed using grid system `X` we either have to reindex these geometries (if that is even possible!) or use the same grid for the polygon tessellation.
# MAGIC 
# MAGIC The best resolution for tessellation is often dictated by the dataset. We want a high resolution without introducing to much skew in the number of tiles needed per polygon.
# MAGIC 
# MAGIC By a rule of thumb it is always better to under index than over index - if not sure select a lower resolution. Higher resolutions are needed when we have very imbalanced geometries with respect to their size or with respect to the number of vertices. In such case indexing with more indices will considerably increase the parallel nature of the operations.
# MAGIC 
# MAGIC Mosaic has built-in functionality to help estimate the optimal index resolution. Let's see that in action.

# COMMAND ----------

import json

with open("../resources/module4/sf_zip_codes.json", "r") as f:
    sf_codes = json.loads(f.read())

neighborhoods = []
for feat in sf_codes["features"]:
    geom = feat["geometry"]["geometries"][0]
    neighborhood = feat["properties"]["neighborhood"]
    neighborhoods.append((neighborhood, json.dumps(geom)))

df_neighborhoods = spark.createDataFrame(neighborhoods, "name STRING, geom_geojson STRING").withColumn("geom", mos.st_geomfromgeojson("geom_geojson"))
display(df_neighborhoods)

# COMMAND ----------

from mosaic import MosaicFrame

neighbourhoods_mosaic_frame = MosaicFrame(df_neighborhoods, "geom")
optimal_resolution = neighbourhoods_mosaic_frame.get_optimal_resolution(sample_fraction=0.75)

print(f"Optimal resolution is {optimal_resolution}")

# COMMAND ----------

display(neighbourhoods_mosaic_frame.get_resolution_metrics(sample_rows=23))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tessellate using the optimal resolution
# MAGIC Now that we have the optimal resolution, let's tessellate the geometries. Let's first take a look at the geometries before the tessellation

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_neighborhoods "geom" "geometry"

# COMMAND ----------

# Now tessellate the neighborhoods
df_tessellated = (
    df_neighborhoods
    .withColumn("tessellated", mos.grid_tessellateexplode("geom", F.lit(8)))
    .withColumn("neighborhood_cell_id", F.col("tessellated.index_id"))
    .withColumn("chip_geom", F.col("tessellated.wkb"))
    .withColumn("is_core", F.col("tessellated.is_core"))
)

display(df_tessellated)

# COMMAND ----------

# MAGIC %md
# MAGIC now visualize the chipped neighborhoods

# COMMAND ----------

df_viz = df_tessellated.select("chip_geom")

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_viz "chip_geom" "geometry" 10000

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hybrid point-in-polygon join

# COMMAND ----------

# Index offices using the same resolution as tessellation
offices = [
    ("Databricks HQ", "POINT (-122.39363 37.79125)"),
    ("Consulate Japan", "POINT (-122.400381 37.794605)")
]

df_offices = spark.createDataFrame(offices, "office STRING, geom_wkt STRING").withColumn("office_cell_id", h3_pointash3("geom_wkt", F.lit(8)))

# COMMAND ----------

# MAGIC %md
# MAGIC Now that the points and neighborhoods are indexed we can find out to which neighborhoods the offices belong. There are several strategies:
# MAGIC 
# MAGIC - "Overestimate": join neighborhood chips on office cells. This will yield too many matches
# MAGIC - "Underestimate": join on neighborhood _core_ chips on office cells. This will yield too little matches
# MAGIC - "Precise": join on neighborhood chips on office cells then run a `st_contains` filter on boundary chips. 

# COMMAND ----------

df_overestimate = df_tessellated.join(
    df_offices, df_tessellated.neighborhood_cell_id == df_offices.office_cell_id
).select("name", "office")
display(df_overestimate)

# COMMAND ----------

df_underestimate = (
    df_tessellated.join(
        df_offices, df_tessellated.neighborhood_cell_id == df_offices.office_cell_id
    )
    .filter(df_tessellated.is_core)
    .select("name", "office")
)
display(df_underestimate)

# COMMAND ----------

df_precise = df_tessellated.join(
    df_offices, df_tessellated.neighborhood_cell_id == df_offices.office_cell_id
).filter(
    df_tessellated.is_core
    | mos.st_contains(df_tessellated.chip_geom, df_offices.geom_wkt)
).select("name", "office")
display(df_precise)
