# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />  
# MAGIC 
# MAGIC  
# MAGIC   Name:          chapter 05/00 - Chapter 5 Initialization
# MAGIC 
# MAGIC     Purpose:  The notebooks in this folder contains the code for chapter 5 of the book - Performance Tuning.
# MAGIC               This notebook resets and sets up all Hive databases and data files, so that we can successfully 
# MAGIC               execute all notebooks in this chapter in sequence.
# MAGIC 
# MAGIC                 
# MAGIC     The following actions are taken in this notebook:
# MAGIC      1 - Drop the taxidb database with a cascade, deleting all tables in the database
# MAGIC      2 - Copy the YellowTaxisParquet files from DataFiles to the chapter05 directory
# MAGIC      3 - Read the parquet files, and write the table in Delta Format
# MAGIC      4 - Create database and register the delta table in hive
# MAGIC    

# COMMAND ----------

# MAGIC %md 
# MAGIC ###1 - Drop the taxidb database and all of its tables

# COMMAND ----------

# DBTITLE 0,Drop the taxidb database and all of its tables
# MAGIC %sql
# MAGIC drop database if exists taxidb cascade

# COMMAND ----------

# MAGIC %md
# MAGIC ###2 - Copy the YellowTaxisParquet files from DataFiles to the chapter05 directory

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r /mnt/datalake/book/chapter05/YellowTaxisParquet

# COMMAND ----------

# MAGIC %fs
# MAGIC cp -r mnt/datalake/book/DataFiles/YellowTaxisParquet /mnt/datalake/book/chapter05/YellowTaxisParquet

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - Read the parquet files, and write the table in Delta Format

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r /mnt/datalake/book/chapter05/YellowTaxisDelta

# COMMAND ----------

from pyspark.sql.types import (StructType,StructField,StringType,IntegerType,TimestampType,DoubleType,LongType)

# define schema of data
schema = schema = (
    StructType()
    .add("VendorID", LongType(), True)
    .add("tpep_pickup_datetime", TimestampType(), True)
    .add("tpep_dropoff_datetime", TimestampType(), True)
    .add("passenger_count", DoubleType(), True)
    .add("trip_distance", DoubleType(), True)
    .add("RatecodeID", DoubleType(), True)
    .add("store_and_fwd_flag", StringType(), True)
    .add("PULocationID", LongType(), True)
    .add("DOLocationID", LongType(), True)
    .add("payment_type", LongType(), True)
    .add("fare_amount", DoubleType(), True)
    .add("extra", DoubleType(), True)
    .add("mta_tax", DoubleType(), True)
    .add("tip_amount", DoubleType(), True)
    .add("tolls_amount", DoubleType(), True)
    .add("total_amount", DoubleType(), True)
    .add("congestion_surcharge", DoubleType(), True)
    .add("airport_fee", DoubleType(), True)
)

# read multiple years of parquet files
years = [2021, 2020]

# create blank dataframe
df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

# for each item in the year list, combine the data into a single dataframe
for i in years:
    # union dataframes with each year of data together
    df = df.union(
        spark.read.format("parquet")
        .schema(schema)
        .load(f"/mnt/datalake/book/chapter05/YellowTaxisParquet/{i}")
    )

# write delta table
df.write.format("delta").mode("overwrite").save(
    "/mnt/datalake/book/chapter05/YellowTaxisDelta/"
)

# COMMAND ----------

#
# insert monthly data from 2022 into the existing delta table.
# 

import os
# get the list of all files and directories
path = "/dbfs/mnt/datalake/book/chapter05/YellowTaxisParquet/2022"
months = os.listdir(path)

for i in months:
    # read monthly parquet file
    df = spark.read.format("parquet").schema(schema).load(
        f"/mnt/datalake/book/chapter05/YellowTaxisParquet/2022/{i}"
    )
    # insert into existing delta table
    df.write.format("delta").mode("append").save(
        "/mnt/datalake/book/chapter05/YellowTaxisDelta/"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - Create database and register the delta table in hive

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS taxidb;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS taxidb.tripData
# MAGIC USING DELTA LOCATION '/mnt/datalake/book/chapter05/YellowTaxisDelta';
