# Databricks notebook source
# MAGIC %md
# MAGIC # Vector data formats
# MAGIC 
# MAGIC Vector data is a representation of specific features on the Earth's surface. Data in this format consists of points, lines or polygons. 
# MAGIC 
# MAGIC In this notebooks we'll learn about the construction of vector data and how to read and write it using Spark.
# MAGIC 
# MAGIC ## Learning objectives
# MAGIC 
# MAGIC By the end of this notebook, you should be able to:
# MAGIC - Explain how geospatial features on the Earth's surface are encoded in the vector format
# MAGIC - Read WKT, WKB and GeoJSON as geometries
# MAGIC - Read longitude/latitude pairs as geometries
# MAGIC - Convert between WKT, WKB and GeoJSON
# MAGIC - Write WKT, WKB and GeoJSON

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Run the setup script. This will import some needed modules and install the mosaic library. The mosaic library and its installation will be explained in depth in module 2.1

# COMMAND ----------

# MAGIC %run ../Includes/Setup-1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Representing geospatial features with vectors
# MAGIC 
# MAGIC The vector format represents the geospatial features on Earth as vectors of discrete geometric locations described by _coordinates_. These coordinates, also known as _vertices_, describe the shape of spatial object. The organization of the vertices determines the type of vector: _points_, _lines_ or _polygons_. Note that the vector format is always an approximation of the feature on earth, e.g. it is impossible to encode a circle using a finite amount of vertices.  Let's bring this definition to live using an example. We'll represent the Databricks HQ in San Fransisco using geospatial vectors. Starting off with an image from above.
# MAGIC 
# MAGIC ![real world geospatial features](files/geospatial/workshop/office_raw.png)
# MAGIC 
# MAGIC _Databricks HQ seen from above (source: Google Earth)_
# MAGIC 
# MAGIC Assume we are interested in three spatial features in this image. The location of the office building, the road behind the office building and the bus stop in front of the building. Let's annotate them using a polygon, line and point respectively.
# MAGIC 
# MAGIC ![annotated geospatial features](files/geospatial/workshop/office_annotated.png)
# MAGIC 
# MAGIC _Annotate the office, street and bus stop using polygons lines and points_
# MAGIC 
# MAGIC Next we need to overlay a coordinate system to express the annotated shapes in terms of coordinates. We chose a coordinate system with the origin in the bottom left of the image.
# MAGIC 
# MAGIC ![annotated features with coordinate system](files/geospatial/workshop/office_coords.png)
# MAGIC 
# MAGIC _Coordinate system overlay with origin in bottom left corner_
# MAGIC 
# MAGIC Finally we can forget about the original image and only keep the vectors representing the spatial objects of interest.
# MAGIC 
# MAGIC ![fully vectorized data](files/geospatial/workshop/office_vector.png)
# MAGIC 
# MAGIC _Geospatial features in vector format_
# MAGIC 
# MAGIC We have now fully encoded the image using a vector format. In this case we have a polygon defined by the coordinates `(2, 4), (2, 8), (6, 8) and (6, 4)`, a line defined by the coordinates `(1, 0) and (1, 10)` and a point defined by the coordinate `(8, 6)`.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## WKT (well-known text representation of geometry)
# MAGIC 
# MAGIC Now that we've seen how vector data is constructed from spatial features, we'll have to talk about the data formats to interpret, transfer and store these vectors. Starting off with WKT is easy, because this format is the easiest for humans to interpret and is essentially a markup language for geospatial vectors. This makes it often the format of choice when creating small example geometries. An elaborate explaination of the format can be found on [wikipedia](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry). Let's load our geometries in Spark using Mosaic.

# COMMAND ----------

# Represent the vector data in the WKT format
point_wkt = "POINT (8 6)"
line_wkt = "LINESTRING (1 0, 1 10)"
polygon_wkt = "POLYGON ((2 4, 2 8, 6 8, 6 4, 2 4))"

df = spark.createDataFrame([("point", point_wkt), ("line", line_wkt), ("polygon", polygon_wkt)], "type STRING, geom_wkt STRING")

# Use the ST_GEOMFROMWKT function to convert into a internal geometry type for processing
df = df.withColumn("geom", mos.st_geomfromwkt(F.col("geom_wkt")))

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## WKB (well-known binary representation of geometry)
# MAGIC 
# MAGIC The WKT format does a good job of being interpretable by humans. In contrast, the WKB is created for machine interpretability. It is also a more compressed than WKT and therefore an excellent format for storing and transfering geoemtries.

# COMMAND ----------

# Represent vector data in WKB format
point_wkb = bytearray(b'\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00 @\x00\x00\x00\x00\x00\x00\x18@')
line_wkb = bytearray(b'\x01\x02\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00$@')
polygon_wkb = bytearray(b'\x01\x03\x00\x00\x00\x01\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00 @\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x10@\x00\x00\x00\x00\x00\x00\x18@\x00\x00\x00\x00\x00\x00\x10@\x00\x00\x00\x00\x00\x00\x18@\x00\x00\x00\x00\x00\x00 @\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00 @')

df = spark.createDataFrame([("point", point_wkb), ("line", line_wkb), ("polygon", polygon_wkb)], "type STRING, geom_wkb BINARY")

# use ST_GEOMFROMWKB to convert into Mosaic's internal geometry format
df = df.withColumn("geom", mos.st_geomfromwkb(F.col("geom_wkb")))

# Compare the `geom` column with the cell above. Are the geometries the same?
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## GeoJSON
# MAGIC 
# MAGIC The GeoJSON is commonly used in web settings. The JSON format is already prevalent for sharing data between clients and servers. The GeoJSON is build upon the JSON standard. When stored the file extension is .geojson (or sometimes just .json extension). A more complete explanation can be found on [wikipedia](https://en.wikipedia.org/wiki/GeoJSON) or the [official standard](https://www.rfc-editor.org/rfc/rfc7946)

# COMMAND ----------

# Vector data in geojson format
point_geojson = '{"type": "Point", "coordinates": [6, 8]}'
line_geojson = '{"type": "LineString", "coordinates": [[1, 0], [1, 10]]}'
polygon_geojson = '{"type": "Polygon", "coordinates": [[[2, 4], [2, 8], [6, 8], [6, 4], [2, 4]]]}'

df = spark.createDataFrame([("point", point_geojson), ("line", line_geojson), ("polygon", polygon_geojson)], "type STRING, geom_geojson STRING")

# use ST_GEOMFROMGEOJSON to convert into Mosaic's internal geometry format
df = df.withColumn("geom", mos.st_geomfromgeojson(F.col("geom_geojson")))

# Compare the `geom` column with the cell above. Are the geometries the same?
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exporting geometries
# MAGIC 
# MAGIC Once we are done with processing geometries in the Mosaic's internal format, we can decide to write them back into one of the mentioned data formats. The [geometry accessors](https://databrickslabs.github.io/mosaic/api/geometry-accessors.html) can be used for this task.

# COMMAND ----------

# Setup a DataFrame containing some geometries
point_wkt = "POINT (8 6)"
line_wkt = "LINESTRING (1 0, 1 10)"
polygon_wkt = "POLYGON ((2 4, 2 8, 6 8, 6 4, 2 4))"

df = spark.createDataFrame([("point", point_wkt), ("line", line_wkt), ("polygon", polygon_wkt)], "type STRING, geom_wkt STRING").withColumn("geom", mos.st_geomfromwkt(F.col("geom_wkt"))).select("geom")

# Convert into the required format
final_df = (
    df
    .withColumn("wkt", mos.st_aswkt(F.col("geom")))
    .withColumn("wkb", mos.st_aswkb(F.col("geom")))
    .withColumn("geojson", mos.st_asgeojson(F.col("geom")))
)

display(final_df)
