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
# MAGIC - Compacting and uncompacting sets of H3 grid cells

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
# MAGIC ## Compacting and uncompacting H3 cells
# MAGIC 
# MAGIC Closely related to grid traversal is the concept of (uncompacting) a set of H3 grid cells. As you may remember, we have already explored a way to find a set of H3 cells that polyfills a polygon. If the polygon is relatively large in comparison with the resolution, the set of grid cells that are completely contained in the original geometry can be rather large. For some operations or visualisations, as well as storage, the large amount of interior grid cells can be wasteful. The solution to this problem is to convert interior cells into their parents, so it takes less cells to store.

# COMMAND ----------

# GeoJSON representation of the polygon describing the location Databricks HQ.
resolution = 12
hq_polygon = {
    "type": "Polygon",
    "coordinates": [[
        [-122.391701, 37.794113],
        [-122.392429, 37.793835],
        [-122.393875, 37.795248],
        [-122.399156, 37.791051],
        [-122.399607, 37.791317],
        [-122.400146, 37.79415],
        [-122.404629, 37.79358],
        [-122.405152, 37.796213],
        [-122.401864, 37.796638],
        [-122.402423, 37.799369],
        [-122.403656, 37.799212],
        [-122.403765, 37.799746],
        [-122.403422, 37.800209],
        [-122.404265, 37.80013],
        [-122.404329, 37.800423],
        [-122.404359, 37.800576],
        [-122.403549, 37.800548],
        [-122.4036, 37.801195],
        [-122.402791, 37.801265],
        [-122.403175, 37.803098],
        [-122.405067, 37.802885],
        [-122.404953, 37.803822],
        [-122.405752, 37.803725],
        [-122.40596, 37.80469],
        [-122.405175, 37.804795],
        [-122.405462, 37.806107],
        [-122.405899, 37.806399],
        [-122.405774, 37.806775],
        [-122.409089, 37.808138],
        [-122.409004, 37.808563],
        [-122.407936, 37.808146],
        [-122.407617, 37.808244],
        [-122.407477, 37.808085],
        [-122.406554, 37.807891],
        [-122.406816, 37.810064],
        [-122.406209, 37.810103],
        [-122.405976, 37.807252],
        [-122.405313, 37.806883],
        [-122.404321, 37.808927],
        [-122.403972, 37.808829],
        [-122.404712, 37.807015],
        [-122.403955, 37.806592],
        [-122.402774, 37.808053],
        [-122.402375, 37.807896],
        [-122.403406, 37.806516],
        [-122.402824, 37.806076],
        [-122.400944, 37.807679],
        [-122.400408, 37.807307],
        [-122.40097, 37.803579],
        [-122.400512, 37.8035],
        [-122.39855, 37.80465],
        [-122.398217, 37.804315],
        [-122.399938, 37.803283],
        [-122.399597, 37.802882],
        [-122.397818, 37.803862],
        [-122.397518, 37.803526],
        [-122.399718, 37.802209],
        [-122.399419, 37.801867],
        [-122.399203, 37.801906],
        [-122.397115, 37.803089],
        [-122.39613, 37.801975],
        [-122.3984, 37.80072],
        [-122.398001, 37.800267],
        [-122.395772, 37.801535],
        [-122.39544, 37.80118],
        [-122.397667, 37.7999],
        [-122.396766, 37.798884],
        [-122.39658, 37.798852],
        [-122.394503, 37.800155],
        [-122.394306, 37.80013],
        [-122.394252, 37.799996],
        [-122.396516, 37.798686],
        [-122.395855, 37.797941],
        [-122.395655, 37.797922],
        [-122.393825, 37.798956],
        [-122.393551, 37.798652],
        [-122.395468, 37.797463],
        [-122.395024, 37.797575],
        [-122.394882, 37.797427],
        [-122.39512, 37.797145],
        [-122.393219, 37.798276],
        [-122.392969, 37.797941],
        [-122.394774, 37.796778],
        [-122.394682, 37.796659],
        [-122.3942, 37.796922],
        [-122.393041, 37.795634],
        [-122.391593, 37.796229],
        [-122.391211, 37.795822],
        [-122.392391, 37.794856],
        [-122.392292, 37.794737],
        [-122.391934, 37.794842],
        [-122.391668, 37.79458],
        [-122.391934, 37.794376],
        [-122.391701, 37.794113],
    ]]
}

df = spark.createDataFrame([(json.dumps(hq_polygon),)], "geom_geojson STRING")
df_traversed = (
    df
    .withColumn("polyfill", h3_polyfillash3(F.col("geom_geojson"), F.lit(resolution)))
    .withColumn("compact", h3_compact(F.col("polyfill")))
    .withColumn("compact_cells", F.explode("compact"))
)

display(df_traversed)

# COMMAND ----------

# MAGIC %md
# MAGIC visualise the compact polyfill 

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_traversed "compact_cells" "h3"

# COMMAND ----------

# 3262 cells in uncompacted polyfill
uncompact_cell_count = len(df_traversed.first().polyfill) 

# compacted has only 850 cells in total, approximately 4x less
df_c = df_traversed.withColumn("resolution_c", h3_resolution("compact_cells")).groupBy("resolution_c").count()

# See distrubution
display(df_c)

# COMMAND ----------

# MAGIC %md
# MAGIC It is always possible to revert back to the origional geometry using `h3_uncompact`.

# COMMAND ----------


