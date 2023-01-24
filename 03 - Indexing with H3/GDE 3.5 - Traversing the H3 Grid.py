# Databricks notebook source
# MAGIC %md
# MAGIC # Traversing the H3 Grid
# MAGIC 
# MAGIC It has already been mentioned that H3 is a grid system, and thus has multiple resolutions. This notebook focuses on the functionality of converting H3 cells to higher/lower resolution
# MAGIC 
# MAGIC ## Learning Goals
# MAGIC - Learn H3 index structure with respect to grid system traversal
# MAGIC - Find parent cell for a cell at a given resolution
# MAGIC - Find child cells for a cell at a given resolution

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Run the cell below to setup mosaic and import H3 functions from DBR

# COMMAND ----------

# MAGIC %run ../Includes/Setup-3

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Finding parents and children of H3 cells
# MAGIC 
# MAGIC In the previous section we already hinted at the possibility of converting H3 cells to higher/lower resolution. It is important to note that H3 is not fully hierarchical (i.e. you cannot recursively partition cells to get to the next resolutions), since you cannot perfectly tile a hexagon with smaller hexagons (try it!). To "fix" this, the subdivision is rotated to make it fit approximately. Every odd numbered resolution has its tiles slightly rotated versus the even numbered resolutions. Therefore the H3 grid is known as a *pseudo-hierarchical* grid system. All the cells within one of the subdivisions of a given H3 cell are called its *children*. Vice verse, every lower resolution cell containing a given H3 cell is called its *parent*.
# MAGIC 
# MAGIC ![Subdivision of H3 grid](https://tdikland.github.io/geospatial-dataengineering-with-databricks/resources/module3/subdivision.png)
# MAGIC 
# MAGIC _Subdivision of a H3 grid cell into its children. Note how the cells have changed their orientation slightly._

# COMMAND ----------

cell_id = "87283082affffff"
cell_resolution = 7

parent_resolution = 5
child_resolution = 9

schema = "h3_cell_id STRING"
df = spark.createDataFrame([{"h3_cell_id": cell_id}], schema)

df_traversed = (
    df
    .withColumn("parent_cell_id", h3_toparent(F.col("h3_cell_id"), F.lit(parent_resolution)))
    .withColumn("child_cell_ids", h3_tochildren(F.col("h3_cell_id"), F.lit(child_resolution)))
)

# How many child cells do you expect to find?
display(df_traversed)

# COMMAND ----------


