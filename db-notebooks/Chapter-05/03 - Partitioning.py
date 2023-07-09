# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />  
# MAGIC 
# MAGIC  
# MAGIC   Name:          chapter 05/03 - Chapter 5 Optimization
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
# MAGIC ###Step 1 - Remove existing Delta table directory to remove all old files

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r /mnt/datalake/book/chapter05/YellowTaxisDelta/

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 2 - Create a partitioned Delta Table

# COMMAND ----------

# DBTITLE 1,Create a partitioned Delta Table
# import month from sql functions
from pyspark.sql.functions import (month, to_date)

# define the source path and destination path
source_path = "/mnt/datalake/book/chapter05/YellowTaxisParquet/2021"
destination_path = "/mnt/datalake/book/chapter05/YellowTaxisDelta/"

# read the delta table, add columns to partitions on, and write it using a partition.
# make sure to overwrite the existing schema if the table already exists since we are adding partitions
spark.read.format("parquet")\
.load(source_path)\
.withColumn('PickupMonth', month('tpep_pickup_datetime'))\
.withColumn('PickupDate', to_date('tpep_pickup_datetime'))\
.write\
.partitionBy('PickupMonth')\
.format("delta")\
.option("overwriteSchema", "true")\
.mode("overwrite")\
.save(destination_path)

# COMMAND ----------

# DBTITLE 1,List partitions for the table
# MAGIC %sql
# MAGIC --list all partitions for the table
# MAGIC SHOW PARTITIONS taxidb.tripData

# COMMAND ----------

# DBTITLE 1,Show partitions in the underlying file system
# import OS module
import os

# list files and directories in directory 
print(os.listdir('/dbfs/'+destination_path))

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 3 - Update specified partitions

# COMMAND ----------

# DBTITLE 1,Use replaceWhere to update a specified partition
# import month from sql functions
from pyspark.sql.functions import (lit, col)
from pyspark.sql.types import (LongType)

spark.read                                                              \
    .format("delta")                                                    \
    .load(destination_path)                                             \
    .where((col("PickupMonth") == '12') & (col("payment_type") == '3')) \
    .withColumn("payment_type", lit(4).cast(LongType()))                \
    .write                                                              \
    .format("delta")                                                    \
    .option("replaceWhere", "PickupMonth = '12'")    \
    .mode("overwrite")                                                  \
    .save(destination_path)

# COMMAND ----------

# DBTITLE 1,Perform compaction on a specified partition
# read a partition from the delta table and repartition it 
spark.read.format("delta")          \
.load(path)                         \
.where((col("PickupMonth") = '12')  \ 
.repartition(5)                     \
.write                              \
.option("dataChange", "false")      \
.format("delta")                    \
.mode("overwrite")                  \
.save(destination_path)

# COMMAND ----------

# DBTITLE 1,Optimize and ZORDER BY on a specified partition
# MAGIC %sql
# MAGIC OPTIMIZE taxidb.tripData WHERE PickupMonth = 12 ZORDER BY tpep_pickup_datetime
