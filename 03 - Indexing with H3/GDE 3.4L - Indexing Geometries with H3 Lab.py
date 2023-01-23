# Databricks notebook source
# MAGIC %md
# MAGIC # Indexing Geometries with H3 Lab

# COMMAND ----------

# Find the geojson representation of the polygon describing the location of your companies HQ and calculate the polyfill.
# TIP: use https://geojson.io to draw a polygon and find its geojson representation.

# TODO
resolution = 13
hq_polygon = {
    "type": "Polygon",
    "coordinates": [
        [
            [4.888213771501597, 52.335756111437945],
            [4.889285938078785, 52.335756111437945],
            [4.889285938078785, 52.33589961572261],
            [4.888213771501597, 52.33589961572261],
            [4.888213771501597, 52.335756111437945],
        ]
    ],
}


schema = "geom_geojson STRING"
df = spark.createDataFrame([{"geom_geojson": json.dumps(hq_polygon)}], schema)
df = df.withColumn("polyfill", h3_polyfillash3(F.col("geom_geojson"), F.lit(resolution)))

display(df)
