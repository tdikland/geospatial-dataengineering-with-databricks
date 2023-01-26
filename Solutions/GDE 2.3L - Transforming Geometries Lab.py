# Databricks notebook source
# MAGIC %md 
# MAGIC # Transforming Geometries Lab
# MAGIC This notebook provides a hands-on review focused on setting up mosaic, transforming and measuring geometries
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab you should be able to
# MAGIC - Install, configure and enable mosaic in the Databricks notebook
# MAGIC - Reproject geometries depending on use case
# MAGIC - Measure geometries using mosaic functions
# MAGIC - Relate geometries using mosaic functions 

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Setting up mosaic

# COMMAND ----------

# MAGIC %md
# MAGIC Install the mosaic library with pip

# COMMAND ----------

# SOLUTION
%pip install databricks-mosaic

# COMMAND ----------

# MAGIC %md
# MAGIC configure mosaic to use `ESRI` and `H3` as geometry api and index system

# COMMAND ----------

# SOLUTION (technically you don't have to do this, this is the default config)
spark.conf.set("spark.databricks.labs.mosaic.geometry.api", "ESRI")
spark.conf.set("spark.databricks.labs.mosaic.index.system", "H3")

# COMMAND ----------

# MAGIC %md
# MAGIC enable the mosaic functions

# COMMAND ----------

# SOLUTION
from mosaic import enable_mosaic
enable_mosaic(spark, dbutils)

import mosaic as mos
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC # Transforming geometries
# MAGIC 
# MAGIC Can you find the perimeter of your companies HQ?

# COMMAND ----------

# SOLUTION (AMS OFFICE)
geom = "POLYGON ((4.888677027629569 52.33591764757733,4.888677027629569 52.335759226198405,4.889275339651931 52.335759226198405,4.889275339651931 52.33591764757733,4.888677027629569 52.33591764757733))"

schema = "geom_wkt STRING"
df = spark.createDataFrame([{"geom_wkt": geom}], schema)
df_hq = df.withColumn("geom", mos.st_geomfromwkt(F.col("geom_wkt")))

# Reproject your geometry if necessary
df_hq = df_hq.withColumn("geom_reprojected", mos.st_transform(F.col("geom"), F.lit(32631)))

# Calculate the perimeter
df_with_area = df_hq.withColumn("perimeter", mos.st_perimeter("geom_reprojected"))

display(df_with_area)

# COMMAND ----------

# MAGIC %md
# MAGIC Does the line between your companies HQ and Databricks HQ cross the equator?

# COMMAND ----------

# Setup geometries
geom_between_hqs = "LINESTRING (-122.393730 37.791252, 4.889083 52.335768)"
geom_equator = "LINESTRING (-180 0, 180 0)"

schema = "geom1_wkt STRING, geom2_wkt STRING"
df = spark.createDataFrame([{"geom1_wkt": geom_between_hqs, "geom2_wkt": geom_equator}], schema)
df_geoms = (df
    .withColumn("geom_hq", mos.st_geomfromwkt(F.col("geom1_wkt")))
    .withColumn("geom_equator", mos.st_geomfromwkt(F.col("geom2_wkt")))
    .select("geom_hq", "geom_equator")
)

# Find the correct predicate
df_predicate = df_geoms.withColumn("crosses_equator", mos.st_intersects("geom_hq", "geom_equator"))

display(df_predicate)

# COMMAND ----------

# MAGIC %md
# MAGIC **!!CHALLANGE!!** 
# MAGIC 
# MAGIC Two telecom providers have several radio towers in the San Fransisco area. These radio towers provide cell phone coverage within 5km of the tower. Given the location of these towers, which telecom provider provides the largest total area of coverage? 
# MAGIC 
# MAGIC HINT: 
# MAGIC - use the ST_BUFFER function
# MAGIC - use the `%%mosaic_kepler` magic to inspect your (intermediate) results

# COMMAND ----------

data = [
    ("T-Mobile", "POINT (-122.426255 37.798106)"),
    ("T-Mobile", "POINT (-122.428550 37.753449)"),
    ("T-Mobile", "POINT (-122.501725 37.762229)"),
    ("T-Mobile", "POINT (-122.501964 37.756874)"),
    ("AT&T", "POINT (-122.410850 37.715457)"),
    ("AT&T", "POINT (-122.499474 37.739015)"),
    ("AT&T", "POINT (-122.414080 37.805109)"),
]

df = spark.createDataFrame(data, "provider STRING, geom_wkt STRING")
df_geoms = df.withColumn("point", mos.st_geomfromwkt(F.col("geom_wkt")))

# SOLUTION
df_buf = (
    df_geoms.withColumn("reprojected_points", mos.st_transform("point", F.lit(32610)))
    .withColumn("coverage", mos.st_buffer("reprojected_points", F.lit(5000)))
    .withColumn("reprojected_coverage", mos.st_transform("coverage", F.lit(4326)))
)

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_buf "reprojected_coverage" "geometry"

# COMMAND ----------

df_cov = df_buf.groupBy("provider").agg(mos.st_union_agg("coverage").alias("cov_union"), mos.st_union_agg("reprojected_coverage").alias("cov_proj_union"))
display(df_cov)

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_cov "cov_proj_union" "geometry"

# COMMAND ----------

df_cov_area = df_cov.withColumn("area_m2", mos.st_area("cov_union"))
display(df_cov_area.select("provider", "area_m2").orderBy(F.col("area_m2").desc()))
