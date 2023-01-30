# Databricks notebook source
# MAGIC %md
# MAGIC # Indexed Spatial Joins Lab
# MAGIC 
# MAGIC This notebook provides a hands-on review on building indexed (gridded) spatial joins.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab you should be able to
# MAGIC - Execute a spatial join using the "indexed" or "gridded" geometric approach
# MAGIC - Create spatial aggregates

# COMMAND ----------

# MAGIC %md
# MAGIC ##Setup
# MAGIC Run the setup script. This will import some needed modules and install the mosaic library. The mosaic library and its installation will be explained in depth in module 2.1

# COMMAND ----------

# MAGIC %run ../Includes/Setup-3

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
# MAGIC We're going to do a similar exercise as in module 2.5, however this time we'll use the H3 gridded approach to do the spatial aggregates. Lets start by indexing and polyfilling the tractors and fields using resolution 11.

# COMMAND ----------

# SOLUTION
df_fields_raw = spark.createDataFrame(fields, "field_id STRING, geom_geojson STRING")
df_tractors_raw = spark.createDataFrame(tractor_positions, "tractor_id STRING, timestamp STRING, longitude DOUBLE, latitude DOUBLE")

df_fields_polyfilled = df_fields_raw.withColumn("polyfilled_idx", F.explode(h3_polyfillash3("geom_geojson", F.lit(11))))
df_tractors_indexed = df_tractors_raw.withColumn("point_idx", h3_longlatash3("longitude", "latitude", F.lit(11)))

display(df_fields_polyfilled)


# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_tractors_indexed "point_idx" "h3"

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_fields_polyfilled "polyfilled_idx" "h3" 10000

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
    df_fields_polyfilled
    .join(df_tractors_indexed, df_fields_polyfilled.polyfilled_idx == df_tractors_indexed.point_idx)
    .groupBy("field_id")
    .agg(F.collect_set("tractor_id").alias("tractor_ids"))
    .orderBy("field_id")
)

display(df_tractors_per_field)

# COMMAND ----------

# MAGIC %md
# MAGIC Is the result the same as in Lab 2.5? Why is/isn't it?
# MAGIC 
# MAGIC Next we're going to do a compact spatial join. First you need to compute the compacted polyfill of your fields.

# COMMAND ----------

# SOLUTION
df_fields_compact = df_fields_raw.withColumn("compact_idx", F.explode(h3_compact(h3_polyfillash3("geom_geojson", F.lit(11)))))

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_fields_compact "compact_idx" "h3"

# COMMAND ----------

# MAGIC %md
# MAGIC Which fields where visited by each tractor? Use the compacted polyfill of the fields. Structure your answer as follows:
# MAGIC | tractor_id | field_ids |
# MAGIC | --- | --- |
# MAGIC | 1 | ["A", "F"] |
# MAGIC | 2 | ["C"] |
# MAGIC | 3 | ["G"] |

# COMMAND ----------

# SOLUTION
same_res_8_parent = h3_toparent(df_tractors_indexed.point_idx, F.lit(8)) == h3_toparent(df_fields_compact.compact_idx, F.lit(8))
is_res_8_field = h3_resolution(df_fields_compact.compact_idx) == F.lit(8)
tractor_is_child_of_field = h3_ischildof(df_tractors_indexed.point_idx, df_fields_compact.compact_idx)

df_fields_per_tractor = (
    df_fields_compact
    .crossJoin(df_tractors_indexed)
    .where(same_res_8_parent & (is_res_8_field | tractor_is_child_of_field))
    .groupBy("tractor_id")
    .agg(F.collect_set("field_id").alias("field_ids"))
    .orderBy("tractor_id")
)

df_fields_per_tractor.explain()

display(df_fields_per_tractor)

# COMMAND ----------

# MAGIC %md
# MAGIC **Bonus question:** which join type was used during the calculations in the cells above? (HINT: use dataframe.explain() to expect the query plan)

# COMMAND ----------

# SOLUTION
# Sort Merge Join
