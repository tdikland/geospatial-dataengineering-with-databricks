# Databricks notebook source
# MAGIC %md
# MAGIC # H3 Grid Distances
# MAGIC 
# MAGIC One of the advantages of H3 hexagonal tiling schema is that all cell centers are (approximately) equidistant. This makes it exceptionally well suited for calculations of distance and "closeness"
# MAGIC 
# MAGIC ## Learning Goals:
# MAGIC - Use H3 distance functions and interpret the results
# MAGIC - Use kRings and hexRings to closeby cells

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Run this cell to setup mosaic and import H3 functions into the notebook

# COMMAND ----------

# MAGIC %run ../Includes/Setup-3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grid distances
# MAGIC We have seen how basic geometries can be mapped to the H3 grid system. One of the properties of the H3 grid system is that the centers of all neighbouring cells lie at the same distance. 
# MAGIC 
# MAGIC ![Equidistant neighbour cells](https://tdikland.github.io/geospatial-dataengineering-with-databricks/resources/module3/neighbours.png)
# MAGIC 
# MAGIC _For a hexagonal grid all neighour cells are equidistant. In square grids there are two possible distances to a neighbour cell and in a triangular grid even three._
# MAGIC 
# MAGIC Since the H3 grid uses the icosahedron face centered gnomonic projection (See GDE 1.1), straight lines in the grid correspond to geodesics (shortest path between points) on the globe. This helps interpreting distance functions in the H3 grid. Let's see this in action.

# COMMAND ----------

# WKT geometry of the office
office = "POINT (-122.39363 37.79125)"

# WKT geometry of some San Fransisco highlights
sf_highlights = [
    ("Golden Gate Bridge", "POINT (-122.47821148 37.81903846)"),
    ("Alcatraz Island", "POINT (-122.42243838 37.826435)"),
    ("Alamo Square", "POINT (-122.43455677 37.77633039)"),
]

resolution = 12
df_office = spark.createDataFrame([(office,)], "office_geom_wkt STRING").withColumn(
    "office_cell", h3_pointash3("office_geom_wkt", F.lit(resolution))
)
df_highlights = spark.createDataFrame(
    sf_highlights, "highlight STRING, highlight_geom_wkt STRING"
).withColumn("highlight_cell", h3_pointash3("highlight_geom_wkt", F.lit(resolution)))

# Calculating the h3 grid distance between the office and the San Fransisco highlights
df_distances = df_office.crossJoin(df_highlights).withColumn("grid_distance", h3_distance("office_cell", "highlight_cell"))

# Check the results, which highlight is closest?
display(df_distances)

# COMMAND ----------

# MAGIC %md 
# MAGIC Let's verify visually

# COMMAND ----------

df_poi = spark.createDataFrame([(office,), *[(h[1],) for h in sf_highlights]], "geom STRING")

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_poi "geom" "geometry"

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Finding closeby cells
# MAGIC 
# MAGIC Above we've measured the distance between different points in H3 grid cells. When the use cases mandates finding geometric features in a certain radius, it is often wastefull to calculate the distance between all the points. We could also calculate all candidate cells that match the search criterium and use those to filter the set of points. The advantage is that if you take the set of cells that are within `k` cells of some origin cell, the resulting set will look like a "disk" around the origin cell. The "disk" is called the `kRing`. This is very useful when answering questions like "find the close points of interest to a given center point".
# MAGIC 
# MAGIC ![Databricks HQ - 1ring](https://tdikland.github.io/geospatial-dataengineering-with-databricks/resources/module3/1ring.png)
# MAGIC ![Databricks HQ - 2ring](https://tdikland.github.io/geospatial-dataengineering-with-databricks/resources/module3/2ring.png)
# MAGIC ![Databricks HQ - 3ring](https://tdikland.github.io/geospatial-dataengineering-with-databricks/resources/module3/3ring.png)
# MAGIC 
# MAGIC _kRings around Databricks HQ with k=1,2,3_

# COMMAND ----------

# Databricks HQ WKT
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

# How many hexagons do you expect in each column?
display(df)
