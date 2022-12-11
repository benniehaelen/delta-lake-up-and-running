# Databricks notebook source
df = spark.read.format("delta").table("taxidb.YellowTaxis")
print(f"Number of records: {df.count():,}")

# COMMAND ----------

# Make sure to import the functions we are using
from pyspark.sql.functions import col, avg, desc

# Read YellowTaxis into our dataframe
df = spark.read.format("delta").table("taxidb.YellowTaxis")

# Perform the group by, average, having and order by equivalents
# in pySpark
results = df.groupBy("CabNumber")                          \
            .agg(avg("FareAmount").alias("AverageFare"))   \
            .filter(col("AverageFare") > 50)               \
            .sort(col("AverageFare").desc())               \
            .take(5)                                      

# Print out the result, since this is a list we can use
# a for loop
[print(result) for result in results]




# COMMAND ----------

print(type(df.groupBy("CabNumber")))
