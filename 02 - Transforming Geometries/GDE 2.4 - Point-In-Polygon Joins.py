# Databricks notebook source
# MAGIC %md
# MAGIC # Point-In-Polygon Joins
# MAGIC 
# MAGIC The so called _point-in-polygon_ is a type of spatial join where a set of polygons is matched with all points which are contained in the respective polygons. In this notebook we'll motivate what problems are solved with this join, and how to execute these in Databricks
# MAGIC 
# MAGIC ## Learning Goals
# MAGIC By the end of this notebook, you should be able to:
# MAGIC - Formulate use cases that require a point-in-polygon join
# MAGIC - Execute a geometric point-in-polygon query using Databricks and mosaic

# COMMAND ----------

# MAGIC %md
# MAGIC ## What does the point-in-polygon join do?
# MAGIC 
# MAGIC The technical answer is not difficult

# COMMAND ----------


