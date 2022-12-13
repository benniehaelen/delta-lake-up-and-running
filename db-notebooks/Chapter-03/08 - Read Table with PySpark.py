# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />   
# MAGIC 
# MAGIC  Name:          chapter 03/08 - Read Table with PySpark
# MAGIC  
# MAGIC      Author:    Bennie Haelen
# MAGIC      Date:      12-10-2022
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 3 of the book - Basic Operations on Delta Tables.
# MAGIC                 This notebook illustrates how to read a table using PySpark
# MAGIC 
# MAGIC                 
# MAGIC      The following Delta Lake functionality is demonstrated in this notebook:
# MAGIC        1 - Using pySpark to get a record count of a Delta Table
# MAGIC        2 - Run a complex query in PySpark
# MAGIC        3 - Illustrate that .groupBy() creates a pyspark.sql.GroupedDate instance
# MAGIC            and not a DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ###1 - Use pySpark to get a record count

# COMMAND ----------

# Note that we can use the .table format to read the entire table
df = spark.read.format("delta").table("taxidb.YellowTaxis")

# Notice the thousands formatter, which makes the result
# a bit easier to read
print(f"Number of records: {df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###2 - Write a complex query in pySpark

# COMMAND ----------

# Make sure to import the functions you want to use
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

# Print out the result, since this is a list and not a DataFrame 
# you can use list comprehension to output the results in a single
# line
[print(result) for result in results]

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - Illustrate that .groupBy() creates a pyspark.sql.GroupedDate instance and not a DataFrame

# COMMAND ----------

# Perform a groupBy, and print out the type
print(type(df.groupBy("CabNumber")))
