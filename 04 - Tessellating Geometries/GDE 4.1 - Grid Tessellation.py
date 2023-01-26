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
# MAGIC - Find a coverage of h3 cells for all polygons. Mark each cell as a `core` cell (completely contained within original polygon) or a boundary cell (intersecting original polygon)
# MAGIC - To check if a point is contained by a polygon, we first check if the point grid cell is part of the core cells (SMJ/HMJ) and then apply the containment filter on the boundary only.
