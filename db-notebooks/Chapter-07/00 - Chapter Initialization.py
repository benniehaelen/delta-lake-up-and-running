# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />  
# MAGIC 
# MAGIC  
# MAGIC   Name:          chapter 07/00 - Chapter Initialization
# MAGIC 
# MAGIC      Author:    Bennie Haelen
# MAGIC      Date:      12-10-2022
# MAGIC      Purpose:   The notebooks in this folder contains the initialization code for chapter 7 of the 
# MAGIC                 book - Updating and Modifying Table Schema
# MAGIC                 This notebook resets all Hive databases and data files, so that we can successfully 
# MAGIC                 execute all notebooks in this chapter in sequence
# MAGIC 
# MAGIC                 
# MAGIC      The following actions are taken in this notebook:
# MAGIC        1 - Drop the taxidb database with a cascade, deleting all tables in the database
# MAGIC        2 - ...
# MAGIC    

# COMMAND ----------

# MAGIC %md 
# MAGIC ###1 - Drop the taxidb database and all of its tables

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists taxidb cascade

# COMMAND ----------

# MAGIC %md
# MAGIC ###2 - Copy the YellowTaxisParquet file from DataFiles to chapter07

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/datalake/book/DataFiles

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r /mnt/datalake/book/chapter07/TaxiRateCode.csv

# COMMAND ----------

# MAGIC %fs
# MAGIC cp mnt/datalake/book/DataFiles/TaxiRateCode.csv /mnt/datalake/book/chapter07/TaxiRateCode.csv

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/datalake/book/chapter07

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - Read the parquet file, and write it out in Delta Format

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/datalake/book/chapter07/TaxiRateCode.delta

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.csv

# COMMAND ----------

# Read a Parquet file
df = spark.read.format("csv")      \
        .option("header", "true") \
        .load("/mnt/datalake/book/chapter07/TaxiRateCode.csv")
df = df.withColumn("RateCodeId", df["RateCodeId"].cast(IntegerType()))

# Write in Delta Lake format
df.write.format("delta")   \
        .mode("overwrite") \
        .save("/mnt/datalake/book/chapter07/TaxiRateCode.delta")

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - Re-create the taxidb database

# COMMAND ----------

# MAGIC %sql
# MAGIC -- First, create the database taxidb
# MAGIC CREATE DATABASE taxidb

# COMMAND ----------

# MAGIC %md
# MAGIC ###5 - Create the YellowTaxis table on top of our Delta File in Chapter07

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Re-create YellowTaxis as an unmanaged table
# MAGIC CREATE TABLE taxidb.TaxiRateCode
# MAGIC (
# MAGIC     RateCodeId              INT,
# MAGIC     RateCodeDesc            STRING
# MAGIC     
# MAGIC ) USING DELTA         
# MAGIC LOCATION "/mnt/datalake/book/chapter07/TaxiRateCode.delta";
# MAGIC 
# MAGIC -- ALTER TABLE taxidb.YellowTaxis
# MAGIC -- ALTER COLUMN RideId COMMENT 'This is the id of the Ride';

# COMMAND ----------

# MAGIC %md
# MAGIC ###6 - Get the count of the table which should be exactly 6 Rows

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  
# MAGIC     COUNT(*)
# MAGIC FROM    
# MAGIC     taxidb.TaxiRateCode

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/00000000000000000000.json
