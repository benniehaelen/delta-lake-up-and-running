# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />  
# MAGIC
# MAGIC  
# MAGIC   Name:          chapter 05/04 - Chapter 5 Liquid Clustering
# MAGIC
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 5 of the book - Performance Tuning.
# MAGIC                 This notebook illustrates liquid clustering.
# MAGIC
# MAGIC      Warning:   At the time of writing, this notebook requires Databricks Runtime 13.2 and above to demonstrate liquid clustering
# MAGIC
# MAGIC                 
# MAGIC      The following actions are taken in this notebook:
# MAGIC        1 - Cleanup, drop table if exists
# MAGIC        2 - Discussion on the role of project controls managers in managing projects internally
# MAGIC        3 - Trigger clustering
# MAGIC        4 - Change cluster columns
# MAGIC        5 - See how table is clustered
# MAGIC        6 - Read data from clustere table
# MAGIC        7 - Remove table clusters
# MAGIC    

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 1 - Cleanup, Drop table if exists

# COMMAND ----------

# DBTITLE 1,Drop table
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS taxidb.tripDataClustered

# COMMAND ----------

# DBTITLE 1,Remove existing Delta table
# MAGIC %fs
# MAGIC rm -r /mnt/datalake/book/chapter05/YellowTaxisLiquidClusteringDelta

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 2 - Create a clustered table and insert data

# COMMAND ----------

# DBTITLE 1,Create a clustered Delta table
# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE taxidb.tripDataClustered CLUSTER BY (VendorId)
# MAGIC LOCATION '/mnt/datalake/book/chapter05/YellowTaxisLiquidClusteringDelta'
# MAGIC AS SELECT * FROM taxiDb.tripData LIMIT 1000;

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 3 - Trigger clustering

# COMMAND ----------

# DBTITLE 1,Trigger clustering on Delta table
# MAGIC %sql
# MAGIC OPTIMIZE taxidb.tripDataClustered;

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 4 - Change cluster columns

# COMMAND ----------

# DBTITLE 1,Change cluster columns
# MAGIC %sql
# MAGIC ALTER TABLE taxidb.tripDataClustered CLUSTER BY (VendorId, RateCodeId);

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 5 - See how table is clustered

# COMMAND ----------

# DBTITLE 1,See how table is clustered
# MAGIC %sql
# MAGIC DESCRIBE TABLE taxidb.tripDataClustered;

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 7 - Read data from clustered table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM taxidb.tripDataClustered WHERE VendorId = 1 and RateCodeId = 1

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 7 - Remove table clusters

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE taxidb.tripDataClustered CLUSTER BY NONE;
