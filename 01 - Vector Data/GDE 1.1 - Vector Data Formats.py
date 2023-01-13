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
# MAGIC - Read WKT, WKB and GeoJson as geometries
# MAGIC - Read longitude/latitude pairs as geometries
# MAGIC - Convert between WKT, WKB and GeoJson
# MAGIC - Write WKT, WKB and GeoJson

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Run the setup script. This will import some needed modules and install the mosaic library. The mosaic library and its installation will be explained in depth in module (TODO)

# COMMAND ----------

# TODO: Run setup script

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
# MAGIC We have now fully encoded the image using a vector format. In this case we have a polygon defined by the coordinates `TODO`, a line defined by the coordinates `TODO` and a point defined by the coordinate `TODO`.

# COMMAND ----------

Vector data is what most people think of when they consider spatial data.  Data in this format consists of points, lines or polygons.  At its simplest level, vector data comprises of individual points stored as coordinate pairs that indicate a physical location in the world.  These points can be joined, in a particular order, to form lines or joined into closed areas to form polygons. Vector data is extremely useful for storing and representing data that has discrete boundaries, such as borders or building footprints, streets and other transport links, and location points.  Ubiquitous online mapping portals, such as Google Maps and Open Street Maps, present data in this format.
