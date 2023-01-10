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

# define the path and number of files to repartition
path = "/mnt/datalake/book/chapter05/YellowTaxisDelta"
numberOfFiles = 1000

# read the delta table and repartition it
spark.read.format("delta").load(path).repartition(numberOfFiles)\
 .write\
 .option("dataChange", "false")\
 .format("delta")\
 .mode("overwrite")\
 .save(path)

# COMMAND ----------

# MAGIC %md 
# MAGIC ###1 - Execute baseline query

# COMMAND ----------

# MAGIC %sql
# MAGIC -- baseline query
# MAGIC -- take note how long it takes to return results
# MAGIC SELECT
# MAGIC   COUNT(*) as count,
# MAGIC   SUM(total_amount) as totalAmount,
# MAGIC   PickupDate
# MAGIC FROM
# MAGIC   taxidb.tripData
# MAGIC WHERE
# MAGIC   PickupDate IN ('2022-01-01', '2021-01-01')
# MAGIC GROUP BY
# MAGIC   PickupDate

# COMMAND ----------

# MAGIC %md 
# MAGIC ###1 - OPTIMIZE and add Z-Ordering to the table

# COMMAND ----------

# DBTITLE 0,Drop the taxidb database and all of its tables
# MAGIC %sql
# MAGIC OPTIMIZE taxidb.tripData Zorder by PickupDate

# COMMAND ----------

# MAGIC %sql
# MAGIC -- baseline query
# MAGIC -- after optimizing the table, note the decrease in time it took to return results compared to query run before
# MAGIC SELECT
# MAGIC   count(*) as count,
# MAGIC   sum(total_amount) as totalAmount,
# MAGIC   PickupDate
# MAGIC FROM
# MAGIC   taxidb.tripData
# MAGIC WHERE
# MAGIC   PickupDate IN ('2021-01-01', '2022-01-01')
# MAGIC GROUP BY
# MAGIC   PickupDate
