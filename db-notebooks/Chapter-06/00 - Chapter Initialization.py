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
# MAGIC DROP DATABASE IF EXISTS taxidb CASCADE

# COMMAND ----------

# MAGIC %md
# MAGIC ###2 - Copy the YellowTaxisParquet files from DataFiles to the chapter05 directory

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r /mnt/datalake/book/chapter06/YellowTaxisParquet

# COMMAND ----------

# MAGIC %fs
# MAGIC cp -r mnt/datalake/book/DataFiles/YellowTaxisParquet /mnt/datalake/book/chapter06/YellowTaxisParquet

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - Read the parquet files, and write the table in Delta Format

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r /mnt/datalake/book/chapter06/YellowTaxisDelta

# COMMAND ----------

df = spark.read.format("parquet").load("/mnt/datalake/book/chapter06/YellowTaxisParquet/2022")
df.write.format("delta").mode("overwrite").save("/mnt/datalake/book/chapter06/YellowTaxisDelta/")

# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - Create database and register the delta table in hive

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS taxidb;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS taxidb.tripData
# MAGIC USING DELTA LOCATION '/mnt/datalake/book/chapter06/YellowTaxisDelta';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxidb.tripData

# COMMAND ----------

# MAGIC %sql
# MAGIC --set log retention to 365 days
# MAGIC ALTER TABLE taxidb.tripData SET TBLPROPERTIES(delta.logRetentionDuration = "interval 365 days");
# MAGIC 
# MAGIC --set data file retention to 365 days
# MAGIC ALTER TABLE taxidb.tripData SET TBLPROPERTIES(delta.deletedFileRetentionDuration = "interval 365 days");
# MAGIC 
# MAGIC --show the table properties to confirm data and log file retention
# MAGIC SHOW TBLPROPERTIES taxidb.tripData;
