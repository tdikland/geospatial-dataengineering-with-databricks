# Databricks notebook source
# MAGIC %md
# MAGIC # Building a Geospatial Data Pipeline Lab
# MAGIC 
# MAGIC In this lab notebook you'll build a geospatial data pipeline following Databricks best practices. Once again we'll be working with the field and tractor data.
# MAGIC 
# MAGIC ## Learning Goals
# MAGIC - Structure a geospatial data pipeline with bronze, silver and gold layers
# MAGIC - Optimize geospatial data for storage in Delta Lake
# MAGIC - Build business aggregates efficiently
# MAGIC - Answer business questions using prepared gold tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Run the following cell to setup dependencies

# COMMAND ----------

# MAGIC %run ../Includes/Setup-5

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Building the bronze layer
# MAGIC 
# MAGIC For the bronze layer we're going to create tables for the tractor and field data

# COMMAND ----------

my_schema = "geospatial_workshop_" + <your initials>  #!!TODO!!
spark.sql(f"CREATE DATABASE IF NOT EXISTS {my_schema}")

# COMMAND ----------

# load the data from "raw" storage
import csv
with open("../resources/module5/fields.csv", "r") as fd:
    data = [field for field in csv.DictReader(fd, delimiter="|")]
    print(data[0])
    df_fields = spark.createDataFrame(data)

df_fields.display()

df_fields.write.mode("overwrite").saveAsTable(...) # TODO

# Create table geospatial_workshop_<your initials>.bronze_fields
# TODO

# COMMAND ----------

# load the data from "raw" storage
with open("../resources/module5/tractor_positions.json") as fd:
    data = [json.loads(line) for line in fd.readlines()]
    df_tractors = spark.createDataFrame(data)

df_tractors.display()

# Create table geospatial_workshop_<your initials>.bronze_tractors
# TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ## Building the silver layer
# MAGIC 
# MAGIC Create the table `geospatial_workshop_<your initials>.silver_fields` with the following properties:
# MAGIC - The columns have the correct data types
# MAGIC - The fields are tessellated with H3 in the optimal resolution and exploded into rows.
# MAGIC - The table is Z-Ordered on the tessellated grid cell id

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md
# MAGIC Create the table `geospatial_workshop_<your initials>.silver_tractors` with the following properties:
# MAGIC - The columns have the correct data types
# MAGIC - The fields are indexed with the same resolution as the fields
# MAGIC - The table is Z-Ordered on the grid cell id and timestamp

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Building the gold layer
# MAGIC 
# MAGIC Create the table `geospatial_workshop_<your initials>.gold_tractors_in_fields` with the following schema
# MAGIC 
# MAGIC | column name | data type | description |
# MAGIC | --- | --- | --- |
# MAGIC | field_id | STRING | The id of the field |
# MAGIC | tractor_id | STRING | The id of the tractor | 
# MAGIC | timestamp | TIMESTAMP | The timestamp of the tractor ping |

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ## Answering business questions
# MAGIC 
# MAGIC Answer the following business questions using the gold table:
# MAGIC - Which fields were plowed between `2023-01-14 01:30:00` and `2023-01-14 02:30:00`
# MAGIC - Which fields were plowed by tractor 1?
# MAGIC - In what order were the fields plowed by tractor 2?
# MAGIC 
# MAGIC **BONUS POINTS** write the queries to answer these questions in SQL

# COMMAND ----------

#TODO
