# Databricks notebook source
def is_runtime_supported():
    True

# COMMAND ----------

# MAGIC %pip install databricks-mosaic

# COMMAND ----------

from pyspark.sql import functions as F
from mosaic import functions as mos
