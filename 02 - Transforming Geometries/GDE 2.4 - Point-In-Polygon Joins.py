# Databricks notebook source
# MAGIC %md
# MAGIC # Point-In-Polygon Joins
# MAGIC 
# MAGIC The so called _point-in-polygon_ is a type of spatial join where a set of polygons is matched with all points which are contained in the respective polygons. In this notebook we'll motivate what problems are solved with this join, and how to execute these in Databricks
# MAGIC 
# MAGIC ![point-in-polygon join](https://tdikland.github.io/geospatial-dataengineering-with-databricks/resources/module2/point-in-polygon.jpeg)
# MAGIC _A graphical representation of a point-in-polygon problem_
# MAGIC 
# MAGIC ## Learning Goals
# MAGIC By the end of this notebook, you should be able to:
# MAGIC - Formulate use cases that require a point-in-polygon join
# MAGIC - Execute a geometric point-in-polygon query using Databricks and mosaic
# MAGIC - Understand the scaling behaviour of the "classical" point-in-polygon join

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Run the setup script. This will import some needed modules and install the mosaic library. The mosaic library and its installation will be explained in depth in module 2.1

# COMMAND ----------

# MAGIC %run ../Includes/Setup-2

# COMMAND ----------

# MAGIC %md
# MAGIC ## What does the point-in-polygon join do?
# MAGIC 
# MAGIC The technical answer is not difficult. A point-in-polygon (PIP) join, matches a dataset of points to a dataset of polygons using the _contains_ relation. The practical use cases are sometimes harder to recognize. Some examples are:
# MAGIC 
# MAGIC - Service coverage analysis
# MAGIC - Retail site planning
# MAGIC - Taxi fleet management
# MAGIC 
# MAGIC What do all of these use cases have on common? How would you leverage the PIP join in these use cases? What use cases within your company follow a similar pattern?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Building a point-in-polygon query
# MAGIC 
# MAGIC Continuing using the toy example, let's see if we can find out which office are visited by Databricks founders. For this example it is assumed that we can track the cell phones of the founders.

# COMMAND ----------

# First of all we create a list of geometries that represent Databricks offices in California
office_geoms = [
    ("HQ", "POLYGON ((-122.39401298500093 37.79129663716428, -122.39401298500093 37.79129663716428, -122.39373988779624 37.79107781442336, -122.39343063138028 37.79132521190732, -122.39370182546828 37.79153952212819, -122.39401298500093 37.79129663716428))"),
    ("Silicon Valley", "POLYGON ((-122.06188699366163 37.38742582735452, -122.06198446313843 37.38724441682429, -122.06179887057283 37.3868975077459, -122.06142501504554 37.386774444812204, -122.06133689195676 37.38694736939405, -122.06151046773734 37.387293217360735, -122.06188699366163 37.38742582735452))")
]

df_offices = (
    spark
    .createDataFrame(office_geoms, "location STRING, office_geom STRING")
    .withColumn("geom", mos.st_geomfromwkt(F.col("office_geom")))
)

# Next we crate a list of points were the cell phones of the founders sent a ping from
founder_pings = [
    ("Ali", "POINT (-122.3940 37.7911)"),
    ("Ali", "POINT (-122.3937 37.79131)"),
    ("Ali", "POINT (-122.3950 37.7911)"),
    ("Matei", "POINT (-122.3937 37.79131)"),
    ("Matei", "POINT (-122.3940 37.7911)"),
    ("Matei", "POINT (-122.0616 37.3870)"),
    ("Reynold", "POINT (-122.3940 37.7911)"),
    ("Reynold", "POINT (-122.3940 37.7911)"),
    ("Reynold", "POINT (-122.3940 37.7911)"),
]

df_pings = (
    spark
    .createDataFrame(founder_pings, "founder STRING, ping STRING")
    .withColumn("geom", mos.st_geomfromwkt(F.col("ping")))
)

# Execute the point-in-polygon join
df_founders_in_offices = (
    df_offices
    .crossJoin(df_pings)
    .where(mos.st_contains(df_offices.geom, df_pings.geom))
    .select("founder", "location")
)

# Which founder prefers to work from home?
display(df_founders_in_offices)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complexity of the solution
# MAGIC 
# MAGIC In terms of code the query above does not look very complex at all! This section is about [time complexity](https://en.wikipedia.org/wiki/Time_complexity) of the PIP join, i.e. how does this computation scale as the number of polygons and points increase. 
# MAGIC 
# MAGIC In this case, if the number of polygons is denoted by `N` and the number of points by `M`, then the `ST_CONTAINS` operations is run `NxM` times. There are more nuances to this, but the bottomline is that this query _does not_ naturally scale to larger datasets. There are techniques to improve the scaling characteristic and they will be covered in the next modules. For now it is important to understand and remember that this query works fine on smaller datasets, but may fail to provide timely insights when the data size increases.
