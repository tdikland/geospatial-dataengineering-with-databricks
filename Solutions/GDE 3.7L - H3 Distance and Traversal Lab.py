# Databricks notebook source
# MAGIC %md
# MAGIC # H3 Distance and Traversal Lab
# MAGIC 
# MAGIC This notebook presents some challanges to review your understanding of the H3 traversal and distance functions. This is done using a fictional use case where gas stations are compared.
# MAGIC 
# MAGIC ## Learning Goals
# MAGIC - Work with H3 traversal functions
# MAGIC - Work with H3 distance functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Run the following cell to setup mosaic and h3 functions

# COMMAND ----------

# MAGIC %run ../Includes/Setup-3

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Find the Best Gas Station Lab
# MAGIC 
# MAGIC Envision the following, you arrive home after a long day of work. You happen to live -very conveniently- at **0 degrees longitude, 0 degrees latitude**. Upon reaching the driveway you look at the gas meter and see that you only have **4L of gas left**. With the current gas prices it is worth it to look for the best deal you can get. Since your car uses **1L of gas for every 50 resolution 9 H3 cell**, you will not be able to reach all of the gas stations as you'll be out of gas before arriving there. You browse the internet and make note of the positions and liter prices of nearby gas stations. Sadly, now every gas station uses the same format for their location. Some use latitude and longitude, some use string representations of H3 cells and some use bigint representations of h3 cells. You have found the following
# MAGIC 
# MAGIC | Brand | Location | Price per Litre |
# MAGIC | --- | --- | --- |
# MAGIC | BP | (-0.090005621 0.005748641) | 2.21 |
# MAGIC | Chevron | (619056576374505471) | 2.05 |
# MAGIC | Exxon | (637070976607846463) | 1.94 |
# MAGIC | Gulf | (8d754e09804003f) | 1.89 |
# MAGIC | Mobil | (0.129706366 0.431238479) | 1.88 |
# MAGIC | Shell | (619056840577908735) | 1.71 |
# MAGIC | Texaco | (8d754ebaa94003f) | 1.42 |
# MAGIC 
# MAGIC Our cars gas tank holds **60L in total**. Once we completed the fueling trip, we want to get home with at least **55L in the tank**. Let's use our knowledge of H3 to spend the least amount of money during the trip!

# COMMAND ----------

# MAGIC %md 
# MAGIC First things first! Let's first cleanup this data. Create a dataframe with the following schema
# MAGIC 
# MAGIC | name | datatype | description |
# MAGIC | --- | --- | --- | 
# MAGIC | brand | STRING | Brand name of the gas station |
# MAGIC | gas_cell_id | BIGINT | H3 cell id of gas station in resolution 9 |
# MAGIC | price | DOUBLE | Price of gas in euros per litre |
# MAGIC 
# MAGIC HINT: use the `h3_resolution` function

# COMMAND ----------

# SOLUTION
lat_lon_row = [
    ("BP", -0.090005621, 0.005748641, 2.21),
    ("Mobil", 0.129706366, 0.431238479, 1.88)
]
df_lat_lon = spark.createDataFrame(lat_lon_row, "brand STRING, lat DOUBLE, lon DOUBLE, price DOUBLE")

hex_idx_rows = [
    ("Gulf", "8d754e09804003f", 1.89),
    ("Texaco", "8d754ebaa94003f", 1.42)
]
df_hex = spark.createDataFrame(hex_idx_rows, "brand STRING, cell_idx STRING, price DOUBLE")

long_idx_rows = [
    ("Chevron", 619056576374505471, 2.05),
    ("Exxon", 637070976607846463, 1.94),
    ("Shell", 619056840577908735, 1.71)
]
df_long = spark.createDataFrame(long_idx_rows, "brand STRING, cell_idx LONG, price DOUBLE")

df_1 = df_lat_lon.withColumn("gas_cell_id", h3_longlatash3("lon", "lat", F.lit(9))).select("brand", "gas_cell_id", "price")
df_2 = df_hex.withColumn("gas_cell_id", h3_toparent(h3_stringtoh3("cell_idx"), F.lit(9))).select("brand", "gas_cell_id", "price")
df_3 = df_long.withColumn("gas_cell_id", h3_toparent("cell_idx", F.lit(9))).select("brand", "gas_cell_id", "price")

df_gas_station = df_1.unionByName(df_2).unionByName(df_3)
df_home = spark.createDataFrame([(619056821840379903,)], "home_cell_id LONG")

display(df_gas_station)

# COMMAND ----------

# MAGIC %md
# MAGIC Use the `mosaic_kepler` magic to visualise the locations of the gas stations cell id and your home location on the map

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_gas_station "gas_cell_id" "h3"

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have an idea of the locations of the gas stations, let's see which gas stations we can reach from our home. Use the `h3_kring` function to find all the cells that can be reached from your home and join this with the gas station dataframe.

# COMMAND ----------

# SOLUTION

max_radius = 50 * 4

df_home_reach = df_home.withColumn("reach", F.explode(h3_kring("home_cell_id", F.lit(max_radius))))
df_reachable_gas_stations = df_home_reach.join(df_gas_station, df_home_reach.reach == df_gas_station.gas_cell_id)

display(df_reachable_gas_stations)

# COMMAND ----------

# MAGIC %md
# MAGIC If we pick the cheapest gas station we can reach and get a full tank of fuel. How much will we have spent and with how much gas do we reach our home? Is this the best we can do?

# COMMAND ----------

# SOLUTION
df_gas_trip = (
    df_reachable_gas_stations.withColumn(
        "trip_fuel_use", h3_distance("home_cell_id", "gas_cell_id") / 50
    )
    .withColumn("fuel_cost", F.col("price") * (60 - 4 + F.col("trip_fuel_use")))
    .withColumn("final_fuel", 60 - F.col("trip_fuel_use"))
)

display(df_gas_trip.select("brand", "fuel_cost", "final_fuel").orderBy("fuel_cost"))

# COMMAND ----------

# MAGIC %md 
# MAGIC **CHALLENGE** 
# MAGIC 
# MAGIC Assume we could also do a two stop trip, getting a bit of expensive fuel to reach a previously out-of-range gas station. What is the best we can do using this strategy? 

# COMMAND ----------

# SOLUTION
fuel_initial = 4
fuel_consumption = 50
fuel_tank = 60

df_two_stops = (
    df_home.crossJoin(df_gas_station.alias("stop1"))
    .crossJoin(df_gas_station.alias("stop2"))
    .withColumn(
        "distance1", h3_distance(F.col("home_cell_id"), F.col("stop1.gas_cell_id"))
    )
    .withColumn(
        "distance2", h3_distance(F.col("stop1.gas_cell_id"), F.col("stop2.gas_cell_id"))
    )
    .withColumn(
        "distance3", h3_distance(F.col("stop2.gas_cell_id"), F.col("home_cell_id"))
    )
    .filter(F.col("distance1") <= fuel_initial * fuel_consumption)
    .filter(F.col("distance1") + F.col("distance2") > fuel_initial * fuel_consumption)
    .withColumn(
        "cost1",
        (F.col("distance1") + F.col("distance2") - fuel_initial * fuel_consumption)
        * (F.col("stop1.price") / fuel_consumption),
    )
    .withColumn("cost2", fuel_tank * F.col("stop2.price"))
    .withColumn("fuel_left", fuel_tank - (F.col("distance3") / fuel_consumption))
    .withColumn("cost_total", F.col("cost1") + F.col("cost2"))
)

df_solution = df_two_stops.withColumn(
    "route", F.concat(F.col("stop1.brand"), F.lit("->"), F.col("stop2.brand"))
).select("route", "cost_total", "fuel_left").orderBy("cost_total")

display(df_solution)
