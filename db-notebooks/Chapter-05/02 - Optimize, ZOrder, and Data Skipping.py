# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />  
# MAGIC 
# MAGIC  
# MAGIC   Name:          chapter 05/02 - Chapter 5 Optimization
# MAGIC 
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 5 of the book - Performance Tuning.
# MAGIC                 This notebook illustrates the OPTIMIZE and Z-Order command, and data skipping.
# MAGIC 
# MAGIC                 
# MAGIC      The following actions are taken in this notebook:
# MAGIC        1 - Repartition the existing Delta Table to 200 files to enable demonstration of OPTIMIZE
# MAGIC        2 - Run OPTIMIZE on Delta Table
# MAGIC        2 - Run OPTIMIZE on the delta table again
# MAGIC        3 - Add partition to the Delta Table and OPTIMIZE subset of data
# MAGIC    

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 1 - Repartition the existing Delta Table to 1000 files to enable demonstration of OPTIMIZE

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

# vaccum the table to remove unneccessrary files generated from the script db-notebooks/Chapter-05/02-Compaction
# you will learn more about the vacuum command in chapter 6
spark.sql('VACUUM taxidb.tripData')

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 2 - Run OPTIMIZE on Delta Table

# COMMAND ----------

# DBTITLE 0,Drop the taxidb database and all of its tables
# MAGIC %sql
# MAGIC -- OPTIMIZE the Delta Table to demonstrate compaction
# MAGIC OPTIMIZE taxidb.tripData

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 3 - Run OPTIMIZE on the Delta Table Again

# COMMAND ----------

# MAGIC %sql
# MAGIC -- run OPTIMIZE on the table again to demonstrate OPTIMIZE is imdepotent
# MAGIC OPTIMIZE taxidb.tripData

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4 - Add partition to the Delta Table and OPTIMIZE subset of data

# COMMAND ----------

# MAGIC %sql 
# MAGIC --replace the existing table by
# MAGIC --adding column to delta table
# MAGIC --to use as a partition
# MAGIC REPLACE TABLE taxidb.tripData USING DELTA PARTITIONED BY (PickupYear) AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   taxidb.tripData

# COMMAND ----------

# MAGIC %sql
# MAGIC --If you have a large amount of data and only want to optimize a subset of it, you can specify an optional partition predicate by using "where".
# MAGIC --OPTIMIZE a specific subset of data using the query below
# MAGIC OPTIMIZE taxidb.tripData WHERE PickupYear = 2022

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 5 - Repartition the existing Delta Table to 1000 files to enable demonstration of OPTIMIZE and Z-ORDER

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

# vaccum the table to remove unneccessrary files generated from the script db-notebooks/Chapter-05/02-Compaction
# you will learn more about the vacuum command in chapter 6
spark.sql('VACUUM taxidb.tripData')

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 6 - Execute baseline query

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
# MAGIC   PickupDate BETWEEN '2022-01-01' AND '2022-03-31'
# MAGIC GROUP BY
# MAGIC   PickupDate

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 7 - OPTIMIZE and add Z-Ordering to the table

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE taxidb.tripData Zorder by PickupDate

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 8 - Execute baseline query again

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
# MAGIC   PickupDate BETWEEN '2022-01-01' AND '2022-03-31'
# MAGIC GROUP BY
# MAGIC   PickupDate

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 9 - Show file statistics used in data skipping

# COMMAND ----------

# MAGIC %sh
# MAGIC # find the last transaction entry and search for "add"
# MAGIC # the output will show you the file stats stored in the json transaction entry for the last file added
# MAGIC grep "\"add"\" "$(ls -1rt /dbfs//mnt/datalake/book/chapter05/YellowTaxisDelta/_delta_log/*.json | tail -n1)" | sed -n 1p > /tmp/commit.json
# MAGIC python -m json.tool < /tmp/commit.json
