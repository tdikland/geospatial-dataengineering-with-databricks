# Databricks notebook source
# MAGIC %md
# MAGIC # Find the Best Gas Station Lab

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
