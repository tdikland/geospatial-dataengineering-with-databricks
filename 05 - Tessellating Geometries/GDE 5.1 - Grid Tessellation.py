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
