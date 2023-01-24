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
# MAGIC - Understand the H3 grid structure and how it helps 

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

cell_id = "85283473fffffff"
cell_resolution = 5

parent_resolution = 3
child_resolution = 7

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

# MAGIC %md
# MAGIC ## Inspecting the H3 cell structure
# MAGIC 
# MAGIC The cell ids are designed such that traversal between parent and child cells is easy and performant. The grid is layed out such that cells are "close" to their spatial neighbours. An example will help to show this works. Focus on the grid cell `85283473fffffff`. As we've already seen we can also represent this using the `BIGINT` representation. A computer will however use the binary representation of the cell id. Lets summarize:
# MAGIC 
# MAGIC | representation | grid cell id |
# MAGIC | --- | --- |
# MAGIC | STRING | 85283473fffffff |
# MAGIC | BIGINT | 599686042433355775 |
# MAGIC | BITS | 0000100001010010100000110100011100111111111111111111111111111111 |
# MAGIC 
# MAGIC The bits translate to grid cells within the H3 grid system as follows:
# MAGIC 
# MAGIC ![index structure](https://tdikland.github.io/geospatial-dataengineering-with-databricks/resources/module3/index_structure.png)
# MAGIC 
# MAGIC _Structure of H3 grid cell ids._
# MAGIC 
# MAGIC The resolution digits indicate which child of the previous resolution is indexed. The base cell is the res 0 index, so we don't need a Res 0 digit. Trailing resolution digits set to 7 are unused. 
# MAGIC 
# MAGIC Let's think about what this means for finding parent/child cells. Finding the parent of a cell is trivial. We could set the bits triples that are no longer needed to specify the lower resolution to 7. Finding the child cells is also easy. we can iterate over all of the bit triples that are "free" between the parent and child resolution.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Min and max child
# MAGIC The section above motivates the following functions, `h3_maxchild` and `h3_minchild`. These functions don't find all child cells at the target (higher) resolution, but only the maximum and minimum cell. This could be useful when doing range joins against non-uniform resolution cell collections. This is something that will be further investigated at the end of this module.

# COMMAND ----------

cell_id = "85283473fffffff"
cell_resolution = 5

child_resolution = 7

schema = "h3_cell_id STRING"
df = spark.createDataFrame([{"h3_cell_id": cell_id}], schema)

df_traversed = (
    df
    .withColumn("max_child_cell_id", h3_maxchild(F.col("h3_cell_id"), F.lit(child_resolution)))
    .withColumn("min_child_cell_id", h3_minchild(F.col("h3_cell_id"), F.lit(child_resolution)))
)

# cross reference your answer with the cells above.
# what is the max/min you expect?
# will the original h3_cell_id be lower, between or above the min-max range?
display(df_traversed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compacting and uncompacting polygons
