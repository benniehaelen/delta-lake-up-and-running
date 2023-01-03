# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />  
# MAGIC 
# MAGIC  
# MAGIC   Name:          chapter 05/01 - Chapter 5 Optimization
# MAGIC 
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 5 of the book - Performance Tuning.
# MAGIC                 This notebook illustrates the OPTIMIZE command and its functionality.
# MAGIC 
# MAGIC                 
# MAGIC      The following actions are taken in this notebook:
# MAGIC        1 - Run OPTIMIZE on Delta Table
# MAGIC        2 - Run OPTIMIZE on the delta table again
# MAGIC        3 - Add partition to the Delta Table and OPTIMIZE subset of data
# MAGIC    

# COMMAND ----------

# MAGIC %md 
# MAGIC ###1 - Run OPTIMIZE on Delta Table

# COMMAND ----------

# DBTITLE 0,Drop the taxidb database and all of its tables
# MAGIC %sql
# MAGIC OPTIMIZE taxidb.tripData

# COMMAND ----------

# MAGIC %md 
# MAGIC ###2 - Run OPTIMIZE on the Delta Table Again

# COMMAND ----------

# MAGIC %sql
# MAGIC -- run OPTIMIZE on the table again to demonstrate OPTIMIZE is imdepotent
# MAGIC OPTIMIZE taxidb.tripData

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - Add partition to the Delta Table and OPTIMIZE subset of data

# COMMAND ----------

# MAGIC %sql 
# MAGIC --replace the existing table by
# MAGIC --adding column to delta table
# MAGIC --to use as a partition
# MAGIC REPLACE TABLE taxidb.tripData USING DELTA PARTITIONED BY (PickupYear) AS
# MAGIC SELECT
# MAGIC   *,
# MAGIC   TO_DATE(tpep_pickup_datetime) AS PickupDate,
# MAGIC   YEAR(tpep_pickup_datetime) AS PickupYear,
# MAGIC   MONTH(tpep_pickup_datetime) AS PickupMonth,
# MAGIC   DAY(tpep_pickup_datetime) AS PickupDay
# MAGIC FROM
# MAGIC   taxidb.tripData

# COMMAND ----------

# MAGIC %sql
# MAGIC --OPTIMIZE a specific subset of data
# MAGIC OPTIMIZE taxidb.tripData WHERE PickupYear >= 2022

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), pickupyear from taxidb.tripData group by PickupYear order by count(*) desc
