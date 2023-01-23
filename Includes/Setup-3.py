# Databricks notebook source
# MAGIC %pip install databricks-mosaic

# COMMAND ----------

from pyspark.sql import functions as F
import mosaic as mos
from mosaic import enable_mosaic

enable_mosaic(spark, dbutils)

# COMMAND ----------

import json
