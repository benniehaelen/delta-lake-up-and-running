# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />  
# MAGIC
# MAGIC  
# MAGIC   Name:          chapter 05/04 - Chapter 5 Optimization
# MAGIC
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 5 of the book - Performance Tuning.
# MAGIC                 This notebook illustrates partitioning.
# MAGIC
# MAGIC                 
# MAGIC      The following actions are taken in this notebook:
# MAGIC        1 - Remove existing Delta table directory to remove all old files
# MAGIC        2 - Create a partitioned Delta Table
# MAGIC        3 - Update specified partitions
# MAGIC    

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 1 - Create a partitioned Delta Table

# COMMAND ----------

# DBTITLE 1,Create a partitioned Delta Table
# import modules
from pyspark.sql.functions import (month, to_date)

## UPDATE destination_path WITH YOUR PATH ##
# define delta table destination path
destination_path = '/mnt/datalake/book/chapter05/YellowTaxisPartionedDelta/'

# read the delta table, add columns to partitions on, and write it using a partition.
# make sure to overwrite the existing schema if the table already exists since we are adding partitions
spark.table('taxidb.tripData')                         \
.withColumn('PickupMonth', month('PickupDate'))        \
.withColumn('PickupDate', to_date('PickupDate'))       \
.write                                                 \
.partitionBy('PickupMonth')                            \
.format("delta")                                       \
.option("overwriteSchema", "true")                     \
.mode("overwrite")                                     \
.save(destination_path)

# register table in hive
spark.sql(f"""CREATE TABLE IF NOT EXISTS taxidb.tripDataPartitioned 
          USING DELTA LOCATION '{destination_path}' """ )

# COMMAND ----------

# DBTITLE 1,List partitions for the table
# MAGIC %sql
# MAGIC --list all partitions for the table
# MAGIC SHOW PARTITIONS taxidb.tripDataPartitioned

# COMMAND ----------

# DBTITLE 1,Show partitions in the underlying file system
# import OS module
import os

# create an environment variable so we can use this variable in the following bash script
os.environ['destination_path'] = '/dbfs' + destination_path  

# list files and directories in directory
print(os.listdir(os.getenv('destination_path')))

# COMMAND ----------

# DBTITLE 1,Show AddFile metadata entry for partition
# MAGIC %sh
# MAGIC # find the last transaction entry and search for "add" to find an added file
# MAGIC # the output will show you the partitionValues
# MAGIC grep "\"add"\" "$(ls -1rt $destination_path/_delta_log/*.json | tail -n1)" | sed -n 1p > /tmp/commit.json | sed -n 1p > /tmp/commit.json
# MAGIC python -m json.tool < /tmp/commit.json

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 2 - Update specified partitions

# COMMAND ----------

# DBTITLE 1,Use replaceWhere to update a specified partition
# import month from sql functions
from pyspark.sql.functions import lit
from pyspark.sql.types import LongType

# use replaceWhere to update a specified partition
spark.read                                                              \
    .format("delta")                                                    \
    .load(destination_path)                                             \
    .where("PickupMonth == '12' and PaymentType == '3' ")               \
    .withColumn("PaymentType", lit(4).cast(LongType()))                 \
    .write                                                              \
    .format("delta")                                                    \
    .option("replaceWhere", "PickupMonth = '12'")                       \
    .mode("overwrite")                                                  \
    .save(destination_path)

# COMMAND ----------

# DBTITLE 1,Perform compaction on a specified partition
# read a partition from the delta table and repartition it 
spark.read.format("delta")          \
.load(destination_path)             \
.where("PickupMonth = '12' ")       \
.repartition(5)                     \
.write                              \
.option("dataChange", "false")      \
.format("delta")                    \
.mode("overwrite")                  \
.save(destination_path)

# COMMAND ----------

# DBTITLE 1,Optimize and ZORDER BY on a specified partition
# MAGIC %sql
# MAGIC OPTIMIZE taxidb.tripData WHERE PickupMonth = 12 ZORDER BY PickupDate
