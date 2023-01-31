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
# MAGIC 
# MAGIC Once again, we'll setup the raw data for the tractors and fields

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

df_fields_raw = spark.createDataFrame(fields, "field_id STRING, geom_geojson STRING")
df_tractors_raw = spark.createDataFrame(tractor_positions, "tractor_id STRING, timestamp STRING, longitude DOUBLE, latitude DOUBLE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyzing the polygons
# MAGIC 
# MAGIC Use the mosaic library to figure out the optimal resolution for tessellation

# COMMAND ----------

from mosaic import MosaicFrame

df_fields_raw = df_fields_raw.withColumn("geom", mos.st_geomfromgeojson("geom_geojson"))

mf_fields = MosaicFrame(df_fields_raw, "geom")
optimal_resolution = mf_fields.get_optimal_resolution(sample_fraction=0.75)

print(f"Optimal resolution is {optimal_resolution}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chipping the polygons and points
# MAGIC 
# MAGIC Use the information from the dataframe analysis to tessellate the fields (polygons) and tractors (points)

# COMMAND ----------

# TODO
# HINT: put every chip of the tessellation on a seperate row
df_fields_tessellated = df_fields_raw.withColumn("tessellated", mos.grid_tessellateexplode("geom", F.lit(9))).select("field_id", "tessellated.*")
df_tractors_indexed = df_tractors_raw.withColumn("tractor_cell_id", mos.grid_longlatascellid("longitude", "latitude", F.lit(9)))

display(df_fields_tessellated)
display(df_tractors_indexed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualising the tessellation
# MAGIC 
# MAGIC Use the `mosaic_kepler` magic to visualise the fields and tractors

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_fields_tessellated "wkb" "geometry" 10000

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_tractors_indexed "tractor_cell_id" "h3"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Point-in-polygon queries
# MAGIC 
# MAGIC Which tractors visited each fields? Use the "precise" join strategy

# COMMAND ----------

df_tractors_per_field = (
    df_fields_tessellated.join(
        df_tractors_indexed,
        df_fields_tessellated.index_id == df_tractors_indexed.tractor_cell_id,
    )
    .filter(
        df_fields_tessellated.is_core
        | mos.st_contains(
            df_fields_tessellated.wkb,
            mos.st_point(df_tractors_indexed.longitude, df_tractors_indexed.latitude),
        )
    )
    .groupBy("field_id")
    .agg(F.collect_set("tractor_id").alias("tractor_ids"))
    .orderBy("field_id")
)

display(df_tractors_per_field)

# COMMAND ----------

# MAGIC %md 
# MAGIC Which fields where visited by each tractor? Give an upper and lower bound.

# COMMAND ----------

df_tractors_in_field = df_fields_tessellated.join(
    df_tractors_indexed,
    df_fields_tessellated.index_id == df_tractors_indexed.tractor_cell_id,
)

df_upper = df_tractors_in_field.groupBy("tractor_id").agg(F.collect_set("field_id").alias("field_ids_upper")).orderBy("tractor_id")
df_lower = df_tractors_in_field.filter(F.col("is_core")).groupBy("tractor_id").agg(F.collect_set("field_id").alias("field_ids_lower")).orderBy("tractor_id")

df_final = df_upper.join(df_lower, "tractor_id")
display(df_final)
