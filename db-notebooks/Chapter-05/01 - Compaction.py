# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />  
# MAGIC 
# MAGIC  
# MAGIC   Name:          chapter 05/01 - Chapter 5 Optimization
# MAGIC 
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 5 of the book - Performance Tuning.
# MAGIC                 This notebook illustrates the how compaction works using the repartition method
# MAGIC 
# MAGIC                 
# MAGIC      The following actions are taken in this notebook:
# MAGIC        1 - Compact the existing delta table
# MAGIC    

# COMMAND ----------

# MAGIC %md 
# MAGIC ###1 - Compact the existing delta table

# COMMAND ----------

# DBTITLE 0,Drop the taxidb database and all of its tables
# define the path and number of files to repartition
path = "/mnt/datalake/book/chapter05/YellowTaxisDelta"
numberOfFiles = 5

# read the delta table and repartition it 
spark.read.format("delta").load(path).repartition(numberOfFiles)\
 .write\
 .option("dataChange", "false")\
 .format("delta")\
 .mode("overwrite")\
 .save(path)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- view numFiles in the output to see how many files the table is comprised of now
# MAGIC describe detail taxidb.tripData
