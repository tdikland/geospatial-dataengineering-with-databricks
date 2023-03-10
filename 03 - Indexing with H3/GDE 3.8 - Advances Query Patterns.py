# Databricks notebook source
# MAGIC %md
# MAGIC # Advanced Query Patterns
# MAGIC 
# MAGIC This notebook covers advanced query patterns such as single resolution, hierarchical and compacted H3 spatial joins
# MAGIC 
# MAGIC ## Learning Goals:
# MAGIC - Build (approximate) point-in-polygon joins using H3
# MAGIC - Understand different spatial join query patterns

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Setup
# MAGIC Run the following cell to setup mosaic and H3 functions

# COMMAND ----------

# MAGIC %run ../Includes/Setup-3

# COMMAND ----------

# We will also define some datasets to be used later on

# Financial district San Fransisco
fd_polygon = {
    "type": "Polygon",
    "coordinates": [[
        [-122.391701, 37.794113],
        [-122.392429, 37.793835],
        [-122.393875, 37.795248],
        [-122.399156, 37.791051],
        [-122.399607, 37.791317],
        [-122.400146, 37.79415],
        [-122.404629, 37.79358],
        [-122.405152, 37.796213],
        [-122.401864, 37.796638],
        [-122.402423, 37.799369],
        [-122.403656, 37.799212],
        [-122.403765, 37.799746],
        [-122.403422, 37.800209],
        [-122.404265, 37.80013],
        [-122.404329, 37.800423],
        [-122.404359, 37.800576],
        [-122.403549, 37.800548],
        [-122.4036, 37.801195],
        [-122.402791, 37.801265],
        [-122.403175, 37.803098],
        [-122.405067, 37.802885],
        [-122.404953, 37.803822],
        [-122.405752, 37.803725],
        [-122.40596, 37.80469],
        [-122.405175, 37.804795],
        [-122.405462, 37.806107],
        [-122.405899, 37.806399],
        [-122.405774, 37.806775],
        [-122.409089, 37.808138],
        [-122.409004, 37.808563],
        [-122.407936, 37.808146],
        [-122.407617, 37.808244],
        [-122.407477, 37.808085],
        [-122.406554, 37.807891],
        [-122.406816, 37.810064],
        [-122.406209, 37.810103],
        [-122.405976, 37.807252],
        [-122.405313, 37.806883],
        [-122.404321, 37.808927],
        [-122.403972, 37.808829],
        [-122.404712, 37.807015],
        [-122.403955, 37.806592],
        [-122.402774, 37.808053],
        [-122.402375, 37.807896],
        [-122.403406, 37.806516],
        [-122.402824, 37.806076],
        [-122.400944, 37.807679],
        [-122.400408, 37.807307],
        [-122.40097, 37.803579],
        [-122.400512, 37.8035],
        [-122.39855, 37.80465],
        [-122.398217, 37.804315],
        [-122.399938, 37.803283],
        [-122.399597, 37.802882],
        [-122.397818, 37.803862],
        [-122.397518, 37.803526],
        [-122.399718, 37.802209],
        [-122.399419, 37.801867],
        [-122.399203, 37.801906],
        [-122.397115, 37.803089],
        [-122.39613, 37.801975],
        [-122.3984, 37.80072],
        [-122.398001, 37.800267],
        [-122.395772, 37.801535],
        [-122.39544, 37.80118],
        [-122.397667, 37.7999],
        [-122.396766, 37.798884],
        [-122.39658, 37.798852],
        [-122.394503, 37.800155],
        [-122.394306, 37.80013],
        [-122.394252, 37.799996],
        [-122.396516, 37.798686],
        [-122.395855, 37.797941],
        [-122.395655, 37.797922],
        [-122.393825, 37.798956],
        [-122.393551, 37.798652],
        [-122.395468, 37.797463],
        [-122.395024, 37.797575],
        [-122.394882, 37.797427],
        [-122.39512, 37.797145],
        [-122.393219, 37.798276],
        [-122.392969, 37.797941],
        [-122.394774, 37.796778],
        [-122.394682, 37.796659],
        [-122.3942, 37.796922],
        [-122.393041, 37.795634],
        [-122.391593, 37.796229],
        [-122.391211, 37.795822],
        [-122.392391, 37.794856],
        [-122.392292, 37.794737],
        [-122.391934, 37.794842],
        [-122.391668, 37.79458],
        [-122.391934, 37.794376],
        [-122.391701, 37.794113],
    ]]
}

offices = [
    ("Databricks HQ", "POINT (-122.39363 37.79125)"),
    ("Consulate Japan", "POINT (-122.400381 37.794605)")
]

df_neighborhood = spark.createDataFrame([("Financial District", json.dumps(fd_polygon))], "neighborhood STRING, geom_geojson STRING")
df_offices = spark.createDataFrame(offices, "office STRING, geom_wkt STRING")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spatial join with single resolution
# MAGIC 
# MAGIC We have already discussed the spatial join (point-in-polygon join) and its applications in module 2 in a classical geometric setting. In the following cells the equivalent calculation is made within the H3 grid domain. Note that by the nature of the grid system, the spatial join in H3 trades accuracy for processing speed.
# MAGIC 
# MAGIC The query pattern does the spatial join in the case that the (h3-polyfilled) polygon and (h3-indexed) points have the same cell resolution.

# COMMAND ----------

# Assume that both the office locations and the neighborhood is buid using resolution 12 cells
resolution = 12
df_offices_single_res = df_offices.withColumn(
    "office_cell_id", h3_pointash3("geom_wkt", F.lit(resolution))
)
df_neighborhood_single_res = df_neighborhood.withColumn(
    "neighborhood_cell_id",
    F.explode(h3_polyfillash3("geom_geojson", F.lit(resolution))),
)

df_pip_single_res = df_neighborhood_single_res.join(
    df_offices_single_res,
    df_offices_single_res.office_cell_id
    == df_neighborhood_single_res.neighborhood_cell_id,
)

display(df_pip_single_res)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spatial join with parent check
# MAGIC 
# MAGIC Assume that the points in the spatial join are indexed on multiple resolutions (e.g. 8, 10 and 12). Adding an extra predicate to the spatial join that filters on a lower resolution can speed up processing.

# COMMAND ----------

# Assume the offices are indexed with multiple resolutions
df_offices_hierarchy = (
    df_offices.withColumn("office_cell_id_8", h3_pointash3("geom_wkt", F.lit(8)))
    .withColumn("office_cell_id_10", h3_pointash3("geom_wkt", F.lit(10)))
    .withColumn("office_cell_id_12", h3_pointash3("geom_wkt", F.lit(12)))
)

# Assume the neighborhood is indexed in resolution 12.
df_neighborhood_hierarchy = df_neighborhood.withColumn(
    "neighborhood_cell_id",
    F.explode(h3_polyfillash3("geom_geojson", F.lit(12))),
)

df_pip_hierarchy = df_neighborhood_hierarchy.join(
    df_offices_hierarchy,
    df_offices_hierarchy.office_cell_id_12
    == df_neighborhood_hierarchy.neighborhood_cell_id,
).where(
    h3_toparent(df_neighborhood_hierarchy.neighborhood_cell_id, F.lit(10))
    == df_offices_hierarchy.office_cell_id_10
)
# In the where clause we do some additional filtering. 
# A general rule of thumb is to pick a parent resolution at or around the minimum resolution you would get when compacting cells.
# Speedup of ~40% have been observed in 

display(df_pip_hierarchy)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spatial join with compactly polyfilled polygons
# MAGIC 
# MAGIC If the polyfill of a polygon is compacted -which can greatly reduce the amount of cells- we need to slightly modify the PIP query

# COMMAND ----------

# Assume the offices are indexed with resolution 8 and 12
df_offices_compact = df_offices.withColumn(
    "office_cell_id_8", h3_pointash3("geom_wkt", F.lit(8))
).withColumn("office_cell_id_12", h3_pointash3("geom_wkt", F.lit(12)))

# Assume the neighborhood is indexed in resolution 12.
df_neighborhood_compact = df_neighborhood.withColumn(
    "neighborhood_compact_cell_id",
    F.explode(h3_compact(h3_polyfillash3("geom_geojson", F.lit(12)))),
)

low_resolution_overlap = h3_toparent(df_neighborhood_compact.neighborhood_compact_cell_id, F.lit(8)) == df_offices_compact.office_cell_id_8
is_low_resolution = h3_resolution(df_neighborhood_compact.neighborhood_compact_cell_id) == 8
is_child = h3_ischildof(df_offices_compact.office_cell_id_12, df_neighborhood_compact.neighborhood_compact_cell_id)

df_pip_compact = df_neighborhood_compact.crossJoin(df_offices_compact).where(low_resolution_overlap & (is_low_resolution | is_child))

display(df_pip_compact)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is the performance of indexed spatial joins?
# MAGIC 
# MAGIC In module 2, the complexity of the "classical" spatial join was discussed. There it was stated that -because we have to run `ST_CONTAINS` on all polygon-point combinations- that spark will plan this query as a cartesian join, which is very expensive in terms of processing latency.
# MAGIC 
# MAGIC The indexed spatial joins that were discussed in this module are so called _equi-joins_ (joins with an "=" predicate), for which spark has better joining strategies. These strategies are for instance broadcast hash join (BHJ), shuffle hash join (SHJ) and sort merge join (SMJ) where the latter two are often used in joins of large datasets. Indexed spatial joins can be planned using these strategies (why?). These joins scale like `O(N LOG(N))` where `N` denotes the amount of points/polygons. This makes a massive difference at scale, as the table below indicates.
# MAGIC 
# MAGIC | N | N * LOG(N) | N * N |
# MAGIC | --- | --- | --- |
# MAGIC | 10 | 33 | 100 |
# MAGIC | 100 | 664 | 10000 |
# MAGIC | 1000 | 9966 | 1000000 |
