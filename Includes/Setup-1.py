# Databricks notebook source
def is_runtime_supported():
    True

# COMMAND ----------

# MAGIC %pip install databricks-mosaic

# COMMAND ----------

from pyspark.sql import functions as F
import mosaic as mos
from mosaic import enable_mosaic

enable_mosaic(spark, dbutils)

# COMMAND ----------


