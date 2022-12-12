# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <span><img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC 
# MAGIC  
# MAGIC  Name:          chapter 03/00 - Chapter 3 Initialization
# MAGIC 
# MAGIC      Author:    Bennie Haelen
# MAGIC      Date:      12-10-2022
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 3 of the book - Basic Operations on Delta Tables.
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

# DBTITLE 0,Drop the taxidb database and all of its tables
# MAGIC %sql
# MAGIC drop database taxidb cascade

# COMMAND ----------

# MAGIC %md
# MAGIC ###2 - Copy the YellowTaxisParquet file from DataFiles to chapter03

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r /mnt/datalake/book/chapter03/YellowTaxisParquet

# COMMAND ----------

# MAGIC %fs
# MAGIC cp mnt/datalake/book/DataFiles/YellowTaxisParquet /mnt/datalake/book/chapter03/YellowTaxisParquet

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - Read the parquet file, and write it out in Delta Format

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r /mnt/datalake/book/chapter03/YellowTaxisDelta

# COMMAND ----------

df = spark.read.format("parquet").load("/mnt/datalake/book/chapter03/YellowTaxisParquet")
df.write.format("delta").mode("overwrite").save("/mnt/datalake/book/chapter03/YellowTaxisDelta/")

# COMMAND ----------

# MAGIC %md
# MAGIC To do:
# MAGIC 1. Make sure that we copy the taxi_rate_code.csv file into the right path
