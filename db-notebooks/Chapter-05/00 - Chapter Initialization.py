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
# MAGIC      2 - Read the parquet files, and write the table in Delta Format
# MAGIC      3 - Create database and register the delta table in hive
# MAGIC    

# COMMAND ----------

# MAGIC %md 
# MAGIC ###1 - Drop the taxidb database and all of its tables

# COMMAND ----------

# DBTITLE 0,Drop the taxidb database and all of its tables
# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS taxidb CASCADE

# COMMAND ----------

# MAGIC %md
# MAGIC ###2 - Read the parquet files, and write the table in Delta Format

# COMMAND ----------

# DBTITLE 1,Read files from dropbox
import pandas as pd
import requests

# define dropbox download url for chapter 05 source file
dropbox_url = 'https://dl.dropboxusercontent.com/s/tgg5s887otj97li/yellow_tripdata_2022.parquet?dl=0'

# read the parquet file from dropbox using pandas then convert to spark dataframe
pandas_df = pd.read_parquet(dropbox_url)
df = spark.createDataFrame(pandas_df)

# COMMAND ----------

# DBTITLE 1,Remove exsiting Delta table
dbutils.fs.rm('/mnt/datalake/book/chapter05/YellowTaxisDelta', True)

# COMMAND ----------

# DBTITLE 1,Write delta table
from pyspark.sql.types import (StructType,StructField,StringType,IntegerType,TimestampType,DoubleType,LongType)
from pyspark.sql.functions import (to_date, year, month, dayofmonth)

# define the path and how many partitions we want this file broken up into so we can demonstrate compaction
path = "/mnt/datalake/book/chapter05/YellowTaxisDelta/"
numberOfFiles = 200

# repartition dataframe and write delta table
df.repartition(numberOfFiles).write.format("delta").mode("overwrite").save(path)

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - Create database and register the delta table in hive

# COMMAND ----------

# DBTITLE 1,Create database
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS taxidb;

# COMMAND ----------

# DBTITLE 1,Register table metadata
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS taxidb.tripData
# MAGIC USING DELTA LOCATION '/mnt/datalake/book/chapter05/YellowTaxisDelta';
