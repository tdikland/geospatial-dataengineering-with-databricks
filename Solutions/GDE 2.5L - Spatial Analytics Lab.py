# Databricks notebook source
# MAGIC %md
# MAGIC # Spatial Analytics Lab
# MAGIC 
# MAGIC This notebook provides a hands-on review on building spatial joins.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab you should be able to
# MAGIC - Execute a spatial join using the "classical" geometric approach
# MAGIC - Create spatial aggregates

# COMMAND ----------

# MAGIC %md
# MAGIC ##Setup
# MAGIC Run the setup script. This will import some needed modules and install the mosaic library. The mosaic library and its installation will be explained in depth in module 2.1

# COMMAND ----------

# MAGIC %run ../Includes/Setup-2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tracking Tractors
# MAGIC 
# MAGIC Suppose a fleet of tractors is fitted with a GPS tracker that sends its location every hour. Let's see if we can figure out what fields a tractor went to!

# COMMAND ----------

# Tractor positions measured using the GPS tracker.
# The tuples represent the tractor_id, measurement time, and latitiude/longitude respectively.
tractor_positions = [
    ("2", "2023-01-14 02:00:00", -92.9559, 42.3954),
    ("2", "2023-01-14 01:00:00", -92.9508, 42.4045),
    ("3", "2023-01-14 01:00:00", -92.9638, 42.3975),
    ("1", "2023-01-14 01:00:00", -92.9481, 42.3873),
    ("3", "2023-01-14 02:00:00", -92.9504, 42.385),
    ("1", "2023-01-14 02:00:00", -92.9508, 42.3883),
    ("1", "2023-01-14 00:00:00", -92.9501, 42.4038),
    ("1", "2023-01-14 00:00:00", -92.9601, 42.3872),
    ("2", "2023-01-14 02:00:00", -92.9526, 42.4036),
    ("3", "2023-01-14 01:00:00", -92.9544, 42.401),
    ("3", "2023-01-14 00:00:00", -92.9577, 42.3884),
    ("2", "2023-01-14 02:00:00", -92.9501, 42.3919),
    ("3", "2023-01-14 01:00:00", -92.9483, 42.3963),
    ("3", "2023-01-14 00:00:00", -92.9466, 42.3848),
    ("2", "2023-01-14 00:00:00", -92.9544, 42.3897),
    ("3", "2023-01-14 02:00:00", -92.9536, 42.3916),
    ("2", "2023-01-14 01:00:00", -92.9518, 42.3995),
    ("1", "2023-01-14 02:00:00", -92.9566, 42.3876),
    ("3", "2023-01-14 02:00:00", -92.9617, 42.3899),
    ("1", "2023-01-14 01:00:00", -92.9502, 42.3951),
    ("1", "2023-01-14 01:00:00", -92.9622, 42.3891),
    ("1", "2023-01-14 00:00:00", -92.9478, 42.3929),
    ("2", "2023-01-14 00:00:00", -92.9572, 42.3873),
    ("3", "2023-01-14 00:00:00", -92.951, 42.3928),
    ("1", "2023-01-14 02:00:00", -92.945, 42.3998),
    ("2", "2023-01-14 00:00:00", -92.9518, 42.3972),
    ("2", "2023-01-14 01:00:00", -92.9588, 42.3862),
]

# The fields on which the tractors work.
# The tuples represent the field_id and the field geometry in geojson format.
fields = [
    (
        "A",
        '{"coordinates": [[[-92.96383588484753, 42.40504553009913], [-92.96383588484753, 42.397895401557605], [-92.95423183265459, 42.397895401557605], [-92.95423183265459, 42.40504553009913], [-92.96383588484753, 42.40504553009913]]], "type": "Polygon"}',
    ),
    (
        "B",
        '{"coordinates": [[[-92.95415311091494, 42.40504553009913], [-92.95415311091494, 42.40155776430916], [-92.94462778046164, 42.40155776430916], [-92.94462778046164, 42.40504553009913], [-92.95415311091494, 42.40504553009913]]], "type": "Polygon"}',
    ),
    (
        "C",
        '{"coordinates": [[[-92.9541601413529, 42.40143038236528], [-92.9541601413529, 42.394308910728085], [-92.9446741717688, 42.394308910728085], [-92.9446741717688, 42.40143038236528], [-92.9541601413529, 42.40143038236528]]], "type": "Polygon"}',
    ),
    (
        "D",
        '{"coordinates": [[[-92.9541601413529, 42.39422170402594], [-92.9541601413529, 42.38361065101748], [-92.9446741717688, 42.38361065101748], [-92.9446741717688, 42.39422170402594], [-92.9541601413529, 42.39422170402594]]], "type": "Polygon"}',
    ),
    (
        "E",
        '{"coordinates": [[[-92.96400035876349, 42.397855213953534], [-92.96400035876349, 42.39076240710219], [-92.95888344570996, 42.39076240710219], [-92.95888344570996, 42.397855213953534], [-92.96400035876349, 42.397855213953534]]], "type": "Polygon"}',
    ),
    (
        "F",
        '{"coordinates": [[[-92.95880472397073, 42.39779707945081], [-92.95880472397073, 42.3906751954722], [-92.95435694570094, 42.3906751954722], [-92.95435694570094, 42.39779707945081], [-92.95880472397073, 42.39779707945081]]], "type": "Polygon"}',
    ),
    (
        "G",
        '{"coordinates": [[[-92.96396099789388, 42.39052984248633], [-92.96396099789388, 42.38361065101748], [-92.95427822396172, 42.38361065101748], [-92.95427822396172, 42.39052984248633], [-92.96396099789388, 42.39052984248633]]], "type": "Polygon"}',
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC Start off by creating dataframes with geometries specified above

# COMMAND ----------

fields_schema = "field_id STRING, geom_geojson STRING"
df_fields_raw = spark.createDataFrame(fields, fields_schema)

tractors_schema = "tractor_id STRING, timestamp STRING, latitude DOUBLE, longitude DOUBLE"
df_tractors_raw = spark.createDataFrame(tractor_positions, tractors_schema)

# Extract geometry. 
df_fields = df_fields_raw.withColumn("field_geom", mos.st_geomfromgeojson("geom_geojson")) #SOLUTION
df_tractors = df_tractors_raw.withColumn("tractor_geom", mos.st_point("latitude", "longitude")).withColumn("ts", F.to_timestamp("timestamp")) #SOLUTION

display(df_fields)
display(df_tractors)

# COMMAND ----------

# MAGIC %md
# MAGIC visualise the geometries using the mosaic_kepler magic command

# COMMAND ----------

df_field_geom = df_fields.withColumn("geom", F.col("field_geom")).select("geom")
df_tractor_geom = df_tractors.withColumn("geom", F.col("tractor_geom")).select("geom")
df_union = df_field_geom.unionByName(df_tractor_geom)

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_union "geom" "geometry"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Which tractors visited each fields? Structure your answer as follows:
# MAGIC | field_id | tractor_ids |
# MAGIC | --- | --- |
# MAGIC | A | [1, 3] |
# MAGIC | B | [] |
# MAGIC | C | [2] |

# COMMAND ----------

# SOLUTION
df_tractors_per_field = (
    df_fields.crossJoin(df_tractors)
    .where(mos.st_contains("field_geom", "tractor_geom"))
    .groupBy("field_id")
    .agg(F.collect_set("tractor_id").alias("tractor_ids"))
    .orderBy("field_id")
)

display(df_tractors_per_field)

# COMMAND ----------

# MAGIC %md
# MAGIC Which fields where visited by each tractor? Structure your answer as follows:
# MAGIC | tractor_id | field_ids |
# MAGIC | --- | --- |
# MAGIC | 1 | ["A", "F"] |
# MAGIC | 2 | ["C"] |
# MAGIC | 3 | ["G"] |

# COMMAND ----------

# SOLUTION
df_fields_per_tractor = (
    df_fields.crossJoin(df_tractors)
    .where(mos.st_contains("field_geom", "tractor_geom"))
    .groupBy("tractor_id")
    .agg(F.collect_set("field_id").alias("field_ids"))
    .orderBy("tractor_id")
)

display(df_fields_per_tractor)

# COMMAND ----------

# MAGIC %md
# MAGIC Find the amount of tractor location pings for each field.

# COMMAND ----------

# SOLUTION
df_pings_per_field = (
    df_fields.crossJoin(df_tractors)
    .where(mos.st_contains("field_geom", "tractor_geom"))
    .groupBy("field_id")
    .count()
    .orderBy("field_id")
)

display(df_pings_per_field)

# COMMAND ----------

# MAGIC %md
# MAGIC **ADVANCED:** find the route of each tractor. (i.e. tractor 1: A -> F -> B -> G)
# MAGIC 
# MAGIC HINT: use a window function after the point in polygon calculation

# COMMAND ----------

# SOLUTION
from pyspark.sql.window import Window

w = Window.orderBy("timestamp").partitionBy("tractor_id")

df_trajectories = (
    df_fields.crossJoin(df_tractors)
    .where(mos.st_contains("field_geom", "tractor_geom"))
    .withColumn("trajectory", F.collect_list("field_id").over(w))
    .groupBy("tractor_id")
    .agg(F.max("trajectory"))
)

display(df_trajectories)
