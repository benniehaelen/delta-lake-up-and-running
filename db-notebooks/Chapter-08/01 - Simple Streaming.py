# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />  
# MAGIC
# MAGIC  
# MAGIC   Name:          chapter 08/ 02 - Simple Streaming
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
# MAGIC        1 - Study the contents of the LimitedRecords Delta Table
# MAGIC        2 - Create a basic streaming query
# MAGIC        3 - Perform an update of the Source Table, triggering a new Micro-Batch
# MAGIC        4 - Take a look at the CheckPoint file
# MAGIC        5 -
# MAGIC    

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1 - Take a look at your "LimitedRecords" Delta Table with 10 records

# COMMAND ----------

# MAGIC %sh
# MAGIC # List the files in our source Delta Table
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter08/LimitedRecords.delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We are starting out with our first 10 records
# MAGIC SELECT * from delta.`/mnt/datalake/book/chapter08/LimitedRecords.delta`

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2 - Create a basic streaming query

# COMMAND ----------

# MAGIC %sh
# MAGIC # Uncomment this line if you want to reset the checkpoint
# MAGIC # There is no need to uncomment this if this is the first time
# MAGIC # you are running the streaming query
# MAGIC # rm -r /dbfs/mnt/datalake/book/chapter08/StreamingTarget/_checkpoint
# MAGIC # rm -r /dbfs/mnt/datalake/book/chapter08/StreamingTarget/

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter08

# COMMAND ----------

# This is the list of columns that we want from our streaming
# DataFrame
select_columns = [
    'RideId', 'VendorId', 'PickupTime', 'DropTime', 
    'PickupLocationId', 'DropLocationId', 'PassengerCount', 
    'TripDistance', 'TotalAmount', 'RecordStreamTime'
]

# COMMAND ----------

# Start streaming from our source "LimitedRecords" table
# Notice that instead of a "read", we now use a "readStream",
# for the rest our statement is just like any other Spark Delta read
stream_df =              \
        spark            \
        .readStream      \
        .format("delta") \
        .load("/mnt/datalake/book/chapter08/LimitedRecords.delta")

# COMMAND ----------

# Add a "RecordStreamTime" column with the timestamp at which we read the 
# record from stream
stream_df = stream_df.withColumn("RecordStreamTime", current_timestamp())

# Select the columns we need. Note that we can manipulate our stream
# just like any other DataStream, although some operations like
# count() are NOT supported, since this is an unbounded DataFrame
stream_df = stream_df.select(select_columns)

# COMMAND ----------

# Define the output location and the checkpoint location
target_location = "/mnt/datalake/book/chapter08/StreamingTarget"
target_checkpoint_location = f"{target_location}/_checkpoint"

# Write the stream to the output location, maintain
# state in the checkpoint location
streamQuery =                                                         \
        stream_df                                                     \
            .writeStream                                              \
            .format("delta")                                          \
            .option("checkpointLocation", target_checkpoint_location) \
            .start(target_location)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/mnt/datalake/book/chapter08/LimitedRecords.delta`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM delta.`/mnt/datalake/book/chapter08/StreamingTarget`

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3 - Perform an update of the Source Table, triggering a new Micro-Batch

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use this query to insert 10 random records from the
# MAGIC -- allYellowTaxis table into the limitedYellowTaxis table
# MAGIC INSERT INTO 
# MAGIC   taxidb.limitedYellowTaxis
# MAGIC SELECT 
# MAGIC   * 
# MAGIC FROM 
# MAGIC   taxidb.allYellowTaxis 
# MAGIC ORDER BY rand() 
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4 - Take a look at the CheckPoint file

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter08/StreamingTarget/_checkpoint/

# COMMAND ----------

# MAGIC %sh
# MAGIC head /dbfs/mnt/datalake/book/chapter08/StreamingTarget/_checkpoint/metadata

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter08/StreamingTarget/_checkpoint/offsets

# COMMAND ----------

# MAGIC %sh
# MAGIC head /dbfs/mnt/datalake/book/chapter08/StreamingTarget/_checkpoint/offsets/0

# COMMAND ----------

# MAGIC %sh
# MAGIC head /dbfs/mnt/datalake/book/chapter08/StreamingTarget/_checkpoint/offsets/1

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/mnt/datalake/book/chapter08/LimitedRecords.delta`

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter08/StreamingTarget/_checkpoint/commits

# COMMAND ----------

# MAGIC %sh
# MAGIC head /dbfs/mnt/datalake/book/chapter08/StreamingTarget/_checkpoint/commits/0

# COMMAND ----------

# MAGIC %sh
# MAGIC head /dbfs/mnt/datalake/book/chapter08/StreamingTarget/_checkpoint/commits/1

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter08/StreamingTarget

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM delta.`/mnt/datalake/book/chapter08/StreamingTarget`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE delta.`/mnt/datalake/book/chapter08/StreamingTarget`
