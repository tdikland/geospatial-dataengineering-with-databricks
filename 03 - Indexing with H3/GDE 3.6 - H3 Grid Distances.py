# Databricks notebook source
# MAGIC %md
# MAGIC # H3 Grid Distances
# MAGIC 
# MAGIC One of the advantages of H3 hexagonal tiling schema is that all cell centers are (approximately) equidistant. This makes it exceptionally well suited for calculations of distance and "closeness"
# MAGIC 
# MAGIC ## Learning Goals:
# MAGIC - 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grid distances
# MAGIC We have seen how basic geometries can be mapped to the H3 grid system. One of the properties of the H3 grid system is that the centers of all neighbouring cells lie at the same distance. 
# MAGIC 
# MAGIC ![Equidistant neighbour cells](https://tdikland.github.io/geospatial-dataengineering-with-databricks/resources/module3/neighbours.png)
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
# MAGIC ![Databricks HQ - 1ring](https://tdikland.github.io/geospatial-dataengineering-with-databricks/resources/module3/1ring.png)
# MAGIC ![Databricks HQ - 2ring](https://tdikland.github.io/geospatial-dataengineering-with-databricks/resources/module3/2ring.png)
# MAGIC ![Databricks HQ - 3ring](https://tdikland.github.io/geospatial-dataengineering-with-databricks/resources/module3/3ring.png)
# MAGIC 
# MAGIC _kRings around Databricks HQ with k=1,2,3_
