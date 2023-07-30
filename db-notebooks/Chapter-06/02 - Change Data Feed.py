# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />  
# MAGIC
# MAGIC  
# MAGIC   Name:          chapter 06/02 - Chapter 6 Time Travel
# MAGIC
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 6 of the book - Using Time Travel.
# MAGIC                 This notebook illustrates the the change data feed works
# MAGIC
# MAGIC                 
# MAGIC      The following actions are taken in this notebook:
# MAGIC        1 - Compact the existing delta table
# MAGIC    

# COMMAND ----------

# MAGIC %md 
# MAGIC ###1 - Create a new table and enable the change data feed

# COMMAND ----------

# DBTITLE 0,Drop the taxidb database and all of its tables
# MAGIC %sql
# MAGIC --create new table with change data feed
# MAGIC CREATE TABLE IF NOT EXISTS taxidb.tripAggregates (VendorId INT, PassengerCount INT, FareAmount INT) 
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/datalake/book/chapter06/TripAggregatesDelta/'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = True);
# MAGIC
# MAGIC --or you can alter existing table to enable change data feed
# MAGIC ALTER TABLE taxidb.tripAggregates SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %md 
# MAGIC ###2 - Modify data in the table to demonstrate the CDF

# COMMAND ----------

# DBTITLE 1,Perform DML Operations to view the CDF
# MAGIC %sql
# MAGIC --update records in the table
# MAGIC UPDATE taxidb.tripAggregates SET FareAmount = 2500 WHERE VendorId = 1;
# MAGIC
# MAGIC -- delete record in the table
# MAGIC DELETE FROM taxidb.tripAggregates WHERE VendorId = 3;
# MAGIC
# MAGIC INSERT INTO taxidb.tripAggregates VALUES 
# MAGIC (4, 500, 1000);

# COMMAND ----------

# DBTITLE 1,View files in the new _change_data directory in the Delta table location
# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter06/TripAggregatesDelta/

# COMMAND ----------

# DBTITLE 1,View files in the _change_data directory
# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter06/TripAggregatesDelta/_change_data

# COMMAND ----------

# DBTITLE 1,View table changes and the CDF using SQL
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM table_changes('taxidb.tripAggregates', 0, 4)
# MAGIC ORDER BY _commit_version

# COMMAND ----------

# DBTITLE 1,View tables changes and the CDF using the DataFrame API
# view CDF table changes
df = spark.read.format("delta") \
  .option("readChangeFeed", "true") \
  .option("startingVersion", 1) \
  .option("endingVersion", 4) \
  .table("taxidb.tripAggregates")

# show results
display(df)  

# COMMAND ----------

# DBTITLE 1,View changes, or and audit trial, of a specific vendor
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM table_changes('taxidb.tripAggregates', 1, 4)
# MAGIC WHERE VendorId = 1 AND _change_type = 'update_postimage'
# MAGIC ORDER BY _commit_timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM table_changes('taxidb.tripAggregates', 1)
# MAGIC --WHERE VendorId = 1 AND _change_type = 'update_postimage'
# MAGIC ORDER BY _commit_timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxidb.tripAggregates 
