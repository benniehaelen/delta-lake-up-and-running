# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />  
# MAGIC
# MAGIC  
# MAGIC   Name:          chapter 07/00 - Chapter Initialization
# MAGIC
# MAGIC      Author:    Bennie Haelen
# MAGIC      Date:      12-10-2022
# MAGIC      Purpose:   The notebooks in this folder contains the initialization code for chapter 8 of the 
# MAGIC                 book - Delta Lake Streaming
# MAGIC                 This notebook resets all Hive databases and data files, so that we can successfully 
# MAGIC                 execute all notebooks in this chapter in sequence
# MAGIC
# MAGIC                 
# MAGIC      The following actions are taken in this notebook:
# MAGIC        1 - Drop the taxidb Database and all of its content
# MAGIC        2 - Clear our the Chapter 08 directory
# MAGIC        3 - Copy over the YellowTaxisParquet file
# MAGIC        4 - Create a large and a small version of our Yellow Taxi Delta Files
# MAGIC        5 - Create a allYellowTaxis and limitedYellowTaxis Delta Table
# MAGIC    

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1 - Drop the taxidb database and all of its content

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists taxidb cascade

# COMMAND ----------

# Let's take a look at whwat files we have available
display(dbutils.fs.ls("mnt/datalake/book/DataFiles"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Clear out the Chapter08 directory

# COMMAND ----------

 dbutils.fs.rm("/mnt/datalake/book/chapter08/", recurse=True)

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/datalake/book/chapter08/")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3 - Copy over the YellowTaxisParquet file

# COMMAND ----------

# MAGIC %fs
# MAGIC cp mnt/datalake/book/DataFiles/YellowTaxisParquet /mnt/datalake/book/chapter08/YellowTaxisParquet

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/datalake/book/chapter08/
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4 - Create a large and a small version of our Yellow Taxi Delta Files

# COMMAND ----------

# MAGIC %sql
# MAGIC -- First, create the database taxidb
# MAGIC CREATE DATABASE taxidb

# COMMAND ----------

# DBTITLE 0,Create a large and small version of our Yellow Taxi Delta Files
#
# Create the Delta files
#

# First read all records
all_records_df = \
    spark        \
        .read     \
        .parquet("/mnt/datalake/book/chapter08/YellowTaxisParquet")

# Now, create a smaller DataFrame with 10 records
limited_records_df = all_records_df.limit(10)

# Write all records the the AllRecords 
all_records_df.write.format("delta").mode("overwrite").save("/mnt/datalake/book/chapter08/AllRecords.delta")

# Write the LimitedRecords table
limited_records_df.write.format("delta").mode("overwrite").save("/mnt/datalake/book/chapter08/LimitedRecords.delta")

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter08/

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 5 - Create a allYellowTaxis and limitedYellowTaxis Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a delta table on both the "all records" Delta file 
# MAGIC -- and the "Limited Records" Delta fil.e
# MAGIC DROP TABLE IF EXISTS taxidb.allYellowTaxis;
# MAGIC DROP TABLE IF EXISTS taxidb.limitedYellowTaxis;
# MAGIC
# MAGIC CREATE TABLE taxidb.allYellowTaxis
# MAGIC USING DELTA 
# MAGIC LOCATION "/mnt/datalake/book/chapter08/AllRecords.delta";
# MAGIC
# MAGIC CREATE TABLE taxidb.limitedYellowTaxis
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/datalake/book/chapter08/LimitedRecords.delta"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM taxidb.limitedyellowtaxis
