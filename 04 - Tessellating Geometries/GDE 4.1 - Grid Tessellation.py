# Databricks notebook source
# MAGIC %md
# MAGIC # Grid Tessellation
# MAGIC 
# MAGIC In module 2, the point-in-polygon join was explored for the first time in the pure geometrical setting. Module 3 showed how the H3 global grid system can be used to approximate geometries and provide a much more performant point-in-polygon join on the grid cell ids (grid indices). These two methods trade accuracy off against processing speed. This module will be focused on _tessellation_ or _chipping_ which is a hybrid technique between the two aforementioned approaches. It is as accurate as the pure geometric method, yet it is much more performant. How this is achieved will be explained in the following notebooks.
# MAGIC 
# MAGIC ## Learning Goals:
# MAGIC - Understand grid tessellation (chipping)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Run the following cell to setup mosaic.

# COMMAND ----------

# MAGIC %run ../Includes/Setup-4

# COMMAND ----------

# MAGIC %md
# MAGIC ## A hybrid approach to indexing
# MAGIC 
# MAGIC In the previous module the H3 DGGS was used to approximate polygons using the `polyfill` operation. The advantage of that approximation is the speedup in processing speed (not uncommon to see x100 speedup on larger datasets). However, an approximation is not okay if precision in the geometries is critical for the use case. Tessellation is a hybrid approach that has the same accuracy as the classical geometric approach, but uses a global grid system to ensure scalability.
# MAGIC 
# MAGIC The idea behind the tessellation approach is as follows:
# MAGIC - Find a coverage of h3 cells for all polygons. Mark each cell as a `core` cell (completely contained within original polygon) or a `boundary` cell (intersecting original polygon)
# MAGIC - Store the intersection of the original geometry and the grid cell for each cell in the polygon cover
# MAGIC 
# MAGIC What results after the _tessellation_ of a polygon is a set of _chips_. Let's tessellate a polygon with mosaic and inspect these chips in more detail.

# COMMAND ----------

# Polygon describing the location Databricks HQ.
hq_polygon = "POLYGON ((-122.391701 37.794113,-122.392429 37.793835,-122.393875 37.795248,-122.399156 37.791051,-122.399607 37.791317,-122.400146 37.79415,-122.404629 37.79358,-122.405152 37.796213,-122.401864 37.796638,-122.402423 37.799369,-122.403656 37.799212,-122.403765 37.799746,-122.403422 37.800209,-122.404265 37.80013,-122.404329 37.800423,-122.404359 37.800576,-122.403549 37.800548,-122.4036 37.801195,-122.402791 37.801265,-122.403175 37.803098,-122.405067 37.802885,-122.404953 37.803822,-122.405752 37.803725,-122.40596 37.80469,-122.405175 37.804795,-122.405462 37.806107,-122.405899 37.806399,-122.405774 37.806775,-122.409089 37.808138,-122.409004 37.808563,-122.407936 37.808146,-122.407617 37.808244,-122.407477 37.808085,-122.406554 37.807891,-122.406816 37.810064,-122.406209 37.810103,-122.405976 37.807252,-122.405313 37.806883,-122.404321 37.808927,-122.403972 37.808829,-122.404712 37.807015,-122.403955 37.806592,-122.402774 37.808053,-122.402375 37.807896,-122.403406 37.806516,-122.402824 37.806076,-122.400944 37.807679,-122.400408 37.807307,-122.40097 37.803579,-122.400512 37.8035,-122.39855 37.80465,-122.398217 37.804315,-122.399938 37.803283,-122.399597 37.802882,-122.397818 37.803862,-122.397518 37.803526,-122.399718 37.802209,-122.399419 37.801867,-122.399203 37.801906,-122.397115 37.803089,-122.39613 37.801975,-122.3984 37.80072,-122.398001 37.800267,-122.395772 37.801535,-122.39544 37.80118,-122.397667 37.7999,-122.396766 37.798884,-122.39658 37.798852,-122.394503 37.800155,-122.394306 37.80013,-122.394252 37.799996,-122.396516 37.798686,-122.395855 37.797941,-122.395655 37.797922,-122.393825 37.798956,-122.393551 37.798652,-122.395468 37.797463,-122.395024 37.797575,-122.394882 37.797427,-122.39512 37.797145,-122.393219 37.798276,-122.392969 37.797941,-122.394774 37.796778,-122.394682 37.796659,-122.3942 37.796922,-122.393041 37.795634,-122.391593 37.796229,-122.391211 37.795822,-122.392391 37.794856,-122.392292 37.794737,-122.391934 37.794842,-122.391668 37.79458,-122.391934 37.794376,-122.391701 37.794113))"

# Tessallate the polygon using Mosaic's grid_tessellateexplode function
df = spark.createDataFrame([(hq_polygon,)], "geom_wkt STRING")
df_tessellated = df.withColumn("tessellated", mos.grid_tessellateexplode("geom_wkt", F.lit(resolution)))

display(df_tessellated)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The chip type
# MAGIC 
# MAGIC In the notebook above the tessellated columns contains structs of the `chip` type. A chip is a collection of three properties, that will be explained below
# MAGIC 
# MAGIC **is_core:** A boolean flag that describes whether the chip is fully contained in the original geometry.
# MAGIC 
# MAGIC **index_id:** The grid cell id that was intersected with this geometry. In the case of H3 this is a 64 bit integer.
# MAGIC 
# MAGIC **wkb:** The intersection geometry of the grid cell and the original geometry. If this is a boundary cell this will not be equal to the geometry of the grid cell. In the case of a core cell it is.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why is tessellation useful?
# MAGIC 
# MAGIC Tessellation has multiple useful properties.
# MAGIC 
# MAGIC Tessellation is **lossless**. The original geometry can be completely reconstructed from its chips, as it is simply the union of all WKB geometries stored in the chips. Contrast this with a polygon that is polyfilled with H3 cells.
# MAGIC 
# MAGIC Transformations on the tessellated geometry can be **decomposed in transformations on the core and boundary cells**. The transformations on the core cells can be done using the grid system, i.e. without doing actual operations on geometries. Only on the WKB geometry of the boundary cells do we need to perform (expensive) operations. If we choose the resolution of the tessellation in a smart way, transformations on a tessellated polygon are much faster than pure geometries (in distributed systems). 

# COMMAND ----------

df_decompose = df_tessellated.withColumn("core_chip", F.col("tessellated.is_core")).groupBy("core_chip").count()

# check the ratio of core cells vs boundary cells
display(df_decompose)

# COMMAND ----------


