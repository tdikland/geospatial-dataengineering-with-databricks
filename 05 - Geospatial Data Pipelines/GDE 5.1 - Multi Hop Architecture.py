# Databricks notebook source
# MAGIC %md
# MAGIC # Multi Hop Architecture
# MAGIC 
# MAGIC In this notebook we'll show the multi-hop architecture (medallion architecture) for a typical geospatial data pipeline.
# MAGIC 
# MAGIC ## Learning goals:
# MAGIC - Understand the medallion architecture for geospatial data pipelines

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Run the following cell to make sure dependencies are installed

# COMMAND ----------

# MAGIC %run ../Includes/Setup-5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Medallion architecture
# MAGIC 
# MAGIC The medallion architecture contains at least 3 levels: bronze, silver and gold. These layers perform roughly the following tasks
# MAGIC 
# MAGIC **Bronze**
# MAGIC The bronze layer is used to store the raw geospatial data in standardized format. Keeping the full (unstructured) data makes it easy to recompute etc.
# MAGIC 
# MAGIC **Silver**
# MAGIC The silver layer makes the data ready for processing. Three major functions of the silver layer are:
# MAGIC - Structured data and fixed schema
# MAGIC - Only valid geometries
# MAGIC - Indexed with a grid system
# MAGIC 
# MAGIC **Gold**
# MAGIC The gold layer makes the geospatial datasets ready for consumption. This layer can:
# MAGIC - Perform point-in-polygon joins
# MAGIC - Make spatial aggregations
# MAGIC - Perform spatial KNN
# MAGIC 
# MAGIC **Finally**
# MAGIC Scientists & analysts can now create rollups, explore geospatial features or windowed aggregates to extract the right insights from the geospatial datasets
# MAGIC 
# MAGIC ![geo pipeline](https://tdikland.github.io/geospatial-dataengineering-with-databricks/resources/module5/geo_pipeline.png)
# MAGIC 
# MAGIC _A typical geospatial pipeline with three hops._

# COMMAND ----------

# MAGIC %md
# MAGIC ## Storing geospatial data in Delta Lake
# MAGIC 
# MAGIC Delta Lake is performant storage format for the Lakehouse. What makes Delta Lake especially well suited for the geospatial Lakehouse is its ability to colocate geospatial features that are also close on the Earth's surface. The Delta Lake features that make this possible are _Z-Ordering_ and _Data Skipping_.
# MAGIC 
# MAGIC ![z-ordering](https://tdikland.github.io/geospatial-dataengineering-with-databricks/resources/module5/z_ordering.png)
# MAGIC 
# MAGIC _A visual explaination of the effectiveness of Z-Ordering._
# MAGIC 
# MAGIC ### Z-Ordering geometries
# MAGIC 
# MAGIC Geometries themself have no clear meaningful order, i.e. sorting them does not necesserily help. Grid system cells however are often structured hierarchically and are therefore excellent candidates for Z-Ordering. Sorting grid cells will colocate spatially "close" cells. Therefore the trick to efficiently store geometries in Delta Lake revolves around tessellation, polyfilling and indexing. Data skipping will then kick in to reduce scanned files in selective queries and speed up joining points and polygons.

# COMMAND ----------

# create a table to store geometries
your_name = "tim"
table_name = "my_example_geometries_" + your_name
create_table = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    geom_wkt STRING,
    cell_id LONG
);
"""

spark.sql(create_table)

# COMMAND ----------

points = ["POINT (0 0)", "POINT (85 47)", "POINT (0 1)", "POINT (1 0)", "POINT (1 1)"]

for p in points:
    df_pnt = spark.createDataFrame([(p,)], "geom_wkt STRING").withColumn("cell_id", h3_pointash3("geom_wkt", F.lit(12)))
    df_pnt.write.mode("append").saveAsTable(table_name)

# COMMAND ----------

optimize_zorder = f"""
OPTIMIZE {table_name} ZORDER BY cell_id;
"""

spark.sql(optimize_zorder)

# COMMAND ----------

describe_history = f"""
DESCRIBE HISTORY {table_name};
"""
display(spark.sql(describe_history))
