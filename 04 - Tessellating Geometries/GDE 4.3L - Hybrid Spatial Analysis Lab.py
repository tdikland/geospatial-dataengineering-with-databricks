# Databricks notebook source
# MAGIC %md
# MAGIC # Hybrid Spatial Analysis Lab
# MAGIC 
# MAGIC In this notebook you're once again going to work with the tractor dataset and perform hybrid point in polygon joins
# MAGIC 
# MAGIC ## Learning goals
# MAGIC - How to tessellate geometries
# MAGIC - How to do hybrid point in polygon joins

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Run the following cell to setup the notebook

# COMMAND ----------

# MAGIC %run ../Includes/Setup-4

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw data

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
        '{"type": "Polygon", "coordinates": [[[-92.96383588484753, 42.40504553009913], [-92.96383588484753, 42.397895401557605], [-92.95423183265459, 42.397895401557605], [-92.95423183265459, 42.40504553009913], [-92.96383588484753, 42.40504553009913]]]}',
    ),
    (
        "B",
        '{"type": "Polygon", "coordinates": [[[-92.95415311091494, 42.40504553009913], [-92.95415311091494, 42.40155776430916], [-92.94462778046164, 42.40155776430916], [-92.94462778046164, 42.40504553009913], [-92.95415311091494, 42.40504553009913]]]}',
    ),
    (
        "C",
        '{"type": "Polygon", "coordinates": [[[-92.9541601413529, 42.40143038236528], [-92.9541601413529, 42.394308910728085], [-92.9446741717688, 42.394308910728085], [-92.9446741717688, 42.40143038236528], [-92.9541601413529, 42.40143038236528]]]}',
    ),
    (
        "D",
        '{"type": "Polygon", "coordinates": [[[-92.9541601413529, 42.39422170402594], [-92.9541601413529, 42.38361065101748], [-92.9446741717688, 42.38361065101748], [-92.9446741717688, 42.39422170402594], [-92.9541601413529, 42.39422170402594]]]}',
    ),
    (
        "E",
        '{"type": "Polygon", "coordinates": [[[-92.96400035876349, 42.397855213953534], [-92.96400035876349, 42.39076240710219], [-92.95888344570996, 42.39076240710219], [-92.95888344570996, 42.397855213953534], [-92.96400035876349, 42.397855213953534]]]}',
    ),
    (
        "F",
        '{"type": "Polygon", "coordinates": [[[-92.95880472397073, 42.39779707945081], [-92.95880472397073, 42.3906751954722], [-92.95435694570094, 42.3906751954722], [-92.95435694570094, 42.39779707945081], [-92.95880472397073, 42.39779707945081]]]}',
    ),
    (
        "G",
        '{"type": "Polygon", "coordinates": [[[-92.96396099789388, 42.39052984248633], [-92.96396099789388, 42.38361065101748], [-92.95427822396172, 42.38361065101748], [-92.95427822396172, 42.39052984248633], [-92.96396099789388, 42.39052984248633]]]}',
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyzing the polygons
# MAGIC 
# MAGIC Use the mosaic library to figure out the optimal resolution for tessellation

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chipping the polygons and points
# MAGIC 
# MAGIC Use the information from the dataframe analysis to tessellate the fields (polygons) and tractors (points)

# COMMAND ----------

# TODO
df_fields_raw = spark.createDataFrame(fields, "field_id STRING, geom_geojson STRING")
df_tractors_raw = spark.createDataFrame(tractor_positions, "tractor_id STRING, timestamp STRING, longitude DOUBLE, latitude DOUBLE")

# HINT: put every chip of the tessellation on a seperate row
df_fields_tessellated = df_fields_raw.withColumn(...)
df_tractors_indexed = df_tractors_raw.withColumn(...)

display(df_fields_tessellated)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualising the tessellation
# MAGIC 
# MAGIC Use the `mosaic_kepler` magic to visualise the fields and tractors

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ## Point-in-polygon queries
# MAGIC 
# MAGIC Which tractors visited each fields? Use the "precise" join strategy

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md 
# MAGIC Which fields where visited by each tractor? Give an upper and lower bound.

# COMMAND ----------

# TODO
