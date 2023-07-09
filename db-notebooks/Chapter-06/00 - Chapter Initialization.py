# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />  
# MAGIC
# MAGIC  
# MAGIC   Name:          chapter 06/00 - Chapter 6 Initialization
# MAGIC
# MAGIC     Purpose:  The notebooks in this folder contains the code for chapter 6 of the book - Using Time Travel.
# MAGIC               This notebook resets and sets up all Hive databases and data files, so that we can successfully 
# MAGIC               execute all notebooks in this chapter in sequence.
# MAGIC
# MAGIC                 
# MAGIC     The following actions are taken in this notebook:
# MAGIC      1 - Drop the taxidb database with a cascade, deleting all tables in the database
# MAGIC      2 - Copy the YellowTaxisParquet files from DataFiles to the chapter06 directory
# MAGIC      3 - Read the parquet files, and write the Delta table
# MAGIC      4 - Create database and register the delta table in hive
# MAGIC      5 - Create a Delta table using a dataframe with custom taxi data
# MAGIC    

# COMMAND ----------

# MAGIC %md 
# MAGIC ###1 - Drop the taxidb database and all of its tables

# COMMAND ----------

# DBTITLE 1,Drop the taxidb database and all of its tables
# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS taxidb CASCADE

# COMMAND ----------

# MAGIC %md
# MAGIC ###2 - Copy the YellowTaxisParquet files from DataFiles to the chapter05 directory

# COMMAND ----------

# DBTITLE 1,Remove existing parquet files
# MAGIC %fs
# MAGIC rm -r /mnt/datalake/book/chapter06/YellowTaxisParquet

# COMMAND ----------

# DBTITLE 1,Remove existing delta table
# MAGIC %fs
# MAGIC rm -r /mnt/datalake/book/chapter06/TripAggregatesDelta

# COMMAND ----------

# DBTITLE 1,Remove existing delta table
# MAGIC %fs
# MAGIC rm -r /mnt/datalake/book/chapter06/YellowTaxisDelta

# COMMAND ----------

# DBTITLE 1,Copy parquet files to this chapter
# MAGIC %fs
# MAGIC cp -r mnt/datalake/book/DataFiles/YellowTaxisParquet /mnt/datalake/book/chapter06/YellowTaxisParquet

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - Read the parquet files, and write to a Delta table

# COMMAND ----------

# DBTITLE 1,Read files and write to Delta table
df = spark.read.format("parquet").load("/mnt/datalake/book/chapter06/YellowTaxisParquet/2022")
df.write.format("delta").mode("overwrite").save("/mnt/datalake/book/chapter06/YellowTaxisDelta/")

# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - Create database and register the delta table in hive

# COMMAND ----------

# DBTITLE 1,Create database
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS taxidb;

# COMMAND ----------

# DBTITLE 1,Register Delta table in Hive
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS taxidb.tripData
# MAGIC USING DELTA LOCATION '/mnt/datalake/book/chapter06/YellowTaxisDelta';

# COMMAND ----------

# MAGIC %md
# MAGIC ###5 - Create aggregate delta table to demonstrate CDF

# COMMAND ----------

# DBTITLE 1,Create Aggregate Delta table
from pyspark.sql.types import StructType,StructField, IntegerType

# specify data and schema for the dataframe
data = [(1, 1000, 2000), (2, 100, 1500), (3, 7000, 10000), (4, 500, 700)]
columns = StructType([ 
                 StructField("VendorId", IntegerType(), True),
                 StructField("PassengerCount", IntegerType(), True),
                 StructField("FareAmount", IntegerType(), True),
             ])

# create delta table
spark.createDataFrame(data=data, schema = columns)\
.write\
.format("delta")\
.option("delta.enableChangeDataFeed", "true")\
.mode("overwrite")\
.save("/mnt/datalake/book/chapter06/TripAggregatesDelta/")
