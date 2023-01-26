# Databricks notebook source
# MAGIC %md 
# MAGIC # Compute Configuration
# MAGIC 
# MAGIC In order to complete the notebooks in this repo, your Databricks compute needs to be configured correctly. This notebook helps setting up a cluster that satisfies these contraints

# COMMAND ----------

# MAGIC %md
# MAGIC ## Navigate to the compute page
# MAGIC 
# MAGIC Navigate to the compute section using the side bar.
# MAGIC 
# MAGIC ![compute page](https://tdikland.github.io/geospatial-dataengineering-with-databricks/resources/module0/cluster_menu.png)
# MAGIC 
# MAGIC TIP: open it up in a seperate browser window to easily switch back and forth during configuration of the compute cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a new cluster
# MAGIC 
# MAGIC Use the `create compute` button to create a new cluster. Make sure that the Databricks Runtime Version is 11.3 LTS and that Photon Acceleration is enabled. Click `create cluster` to start the cluster, it can take a couple of minutes to come up.
# MAGIC 
# MAGIC ![cluster settings](https://tdikland.github.io/geospatial-dataengineering-with-databricks/resources/module0/cluster_settings.png)
