# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />  
# MAGIC 
# MAGIC  
# MAGIC   Name:          chapter 05/02 - Chapter 5 ZOrder
# MAGIC 
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 5 of the book - Performance Tuning.
# MAGIC                 This notebook illustrates Z-ordering and it's functionality.
# MAGIC 
# MAGIC                 
# MAGIC      The following actions are taken in this notebook:
# MAGIC        1 - Drop the taxidb database with a cascade, deleting all tables in the database
# MAGIC        2 - ...
# MAGIC    

# COMMAND ----------

# MAGIC %md 
# MAGIC ###1 - Add Z-Ordering to the table

# COMMAND ----------

# DBTITLE 0,Drop the taxidb database and all of its tables
# MAGIC %sql
# MAGIC OPTIMIZE taxidb.tripData Zorder(PULocationID)

# COMMAND ----------

# MAGIC %md 
# MAGIC ###2 - Run OPTIMIZE on the Delta Table Again

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE taxidb.tripData

# COMMAND ----------

spark.sql("describe detail taxidb.tripData").select("sizeInBytes").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - Add partition to the Delta Table and OPTIMIZE subset of data

# COMMAND ----------

# MAGIC %sql --replace the existing table by
# MAGIC --adding column to delta table
# MAGIC --to use as a partition
# MAGIC REPLACE TABLE taxidb.tripData USING DELTA AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   taxidb.tripData

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE taxidb.tripData ZORDER BY (PickupDate)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), pickupyear from taxidb.tripData group by pickupyear
