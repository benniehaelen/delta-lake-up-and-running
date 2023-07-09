# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />  
# MAGIC
# MAGIC  
# MAGIC   Name:          chapter 06/01 - Chapter 6 Time Travel
# MAGIC
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 6 of the book - Using Time Travel.
# MAGIC                 This notebook illustrates the how compaction works using the repartition method
# MAGIC
# MAGIC                 
# MAGIC      The following actions are taken in this notebook:
# MAGIC        1 - Compact the existing delta table
# MAGIC    

# COMMAND ----------

# MAGIC %md 
# MAGIC ###1 - Update records, Delete records, then Describe table history 

# COMMAND ----------

# DBTITLE 0,Drop the taxidb database and all of its tables
# MAGIC %sql
# MAGIC --update records in table 
# MAGIC UPDATE taxidb.tripData
# MAGIC SET VendorId = 10
# MAGIC WHERE VendorId = 1;
# MAGIC
# MAGIC --update records in table 
# MAGIC DELETE FROM taxidb.tripData
# MAGIC WHERE VendorId = 2; 
# MAGIC
# MAGIC --describe the table history
# MAGIC DESCRIBE HISTORY taxidb.tripData

# COMMAND ----------

# MAGIC %md 
# MAGIC ###2 - Restore table to previous version

# COMMAND ----------

# MAGIC %sql
# MAGIC --restore table to previous version
# MAGIC RESTORE TABLE taxidb.tripData TO VERSION AS OF 0;
# MAGIC
# MAGIC
# MAGIC --describe the table history
# MAGIC DESCRIBE HISTORY taxidb.tripData;

# COMMAND ----------

# import OS module
import os

# count files in directory
print(len(os.listdir('/dbfs/'+ '/mnt/datalake/book/chapter06/YellowTaxisDelta')))

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct vendorid from taxidb.tripData

# COMMAND ----------

version = spark.sql("SELECT max(version) FROM (DESCRIBE HISTORY taxidb.tripData)").collect()

# use the latest version of the table for all operations below
data = spark.table("taxidb.tripData@v%s" % version[0][0])

data.where("VendorId = 1").write.jdbc("table1")
