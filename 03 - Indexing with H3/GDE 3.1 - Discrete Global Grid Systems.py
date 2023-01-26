# Databricks notebook source
# MAGIC %md 
# MAGIC # Discrete Global Grid Systems
# MAGIC 
# MAGIC In this notebook the definition of a _Discrete Global Grid System_ _(DGGS)_ is given and several examples are presented.
# MAGIC 
# MAGIC ## Learning Goals:
# MAGIC - Definition of DGGS
# MAGIC - Examples of DGGS
# MAGIC - Explain some choices that the H3 grid system makes

# COMMAND ----------

# MAGIC %md 
# MAGIC ## What is a discrete global grid system?
# MAGIC 
# MAGIC Let's start off by disecting what each of these words mean.
# MAGIC - *Discrete* means that the grid has a finite amount of cells. Contrast this with the amount of points on the earths surface, which is of infinite size.
# MAGIC - *Global* means that every point on the globe can be associated with a unique grid cell.
# MAGIC - *System* means that the grid is actually a series of dicrete global grids with progressively finer resolution.
# MAGIC 
# MAGIC Some examples of discrete global grid systems (DGGS) are:
# MAGIC - H3
# MAGIC - S2
# MAGIC - Geohash
# MAGIC 
# MAGIC We'll briefly look at the different DGGS. The rest of this module will be focused on the H3 grid system. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### H3
# MAGIC 
# MAGIC The H3 DGGS uses a hexagonal tiles to cover the earth (as well as [12 unavoidable pentagons](https://quantixed.org/2018/06/27/pentagrammarspin-why-twelve-pentagons/)). For map projection, the gnomonic projections centered on icosahedron faces is used. This projects from Earth as a sphere to an icosahedron, a twenty-sided platonic solid.
# MAGIC 
# MAGIC ![H3 map projection](https://tdikland.github.io/geospatial-dataengineering-with-databricks/resources/module3/map_projection.png)
# MAGIC 
# MAGIC _Left: The icosahedron, a solid made out of 20 regular triangles that touch in 12 vertices._
# MAGIC _Middle: The icosahedron faces are projected onto the sphere with a gnomonic projection around the center of the face._ 
# MAGIC _Right: The Earth approximated as a sphere along with the projection of the icosahedron. Note that all vertices are projected into the ocean._
# MAGIC 
# MAGIC This choice of projection has the following consequences:
# MAGIC - The H3 map projection is _not conformal_. Shapes will be distorted.
# MAGIC - The H3 map projection is _not equal-area_. Cells close to the edges of the icosahedron centers will have up to 1.6x more area as cells that are close to the icosahedron edges.
# MAGIC - In the gnomonic projection straight lines are mapped to _geodesics_. This means that the shortest path between two cells in the H3 grid is also the shortest path on earth (something that is _not_ true for the well-known Mercator projection)
# MAGIC 
# MAGIC ![H3 DGGS](https://tdikland.github.io/geospatial-dataengineering-with-databricks/resources/module3/h3.png)
# MAGIC 
# MAGIC _A visualisation of the first three resolutions of the H3 DGGS._

# COMMAND ----------

# MAGIC %md
# MAGIC ### S2
# MAGIC 
# MAGIC The S2 DGGS uses rectangular cells with a cube face centered quadratic transform.
# MAGIC 
# MAGIC ![S2](https://tdikland.github.io/geospatial-dataengineering-with-databricks/resources/module3/s2.gif)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Geohash
# MAGIC 
# MAGIC The Geohash DGGS also uses rectangular cells, and works with longitudes and latitudes.
# MAGIC 
# MAGIC ![geohash](https://tdikland.github.io/geospatial-dataengineering-with-databricks/resources/module3/geohash.jpeg)
