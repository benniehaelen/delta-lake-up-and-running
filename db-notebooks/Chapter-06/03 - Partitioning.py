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
# MAGIC ###Step 1 - Create a partitioned Delta Table

# COMMAND ----------

# import month from sql functions
from pyspark.sql.functions import (month, to_date)


# define the source path and destination path
source_path = "/mnt/datalake/book/chapter05/YellowTaxisParquet/2021"
destination_path = "/mnt/datalake/book/chapter05/YellowTaxisDeltaPartitioned/"

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

# import OS module
import os

# list files and directories in directory 
print(os.listdir('/dbfs/'+destination_path))

# COMMAND ----------

# DBTITLE 1,Use replaceWhere to update a specified partition
# import month from sql functions
from pyspark.sql.functions import (lit)
from pyspark.sql.types import (LongType)

spark.read                                                              \
    .format("delta")                                                    \
    .load(destination_path)                                             \
    .where((col("PickupMonth") == '12') & (col("payment_type") == '3')) \
    .withColumn("payment_type", lit(4).cast(LongType()))                \
    .write                                                              \
    .format("delta")                                                    \
    .option("replaceWhere", "PickupMonth = 12 AND payment_type = 3")    \
    .mode("overwrite")                                                  \
    .save(destination_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE '/mnt/datalake/book/chapter05/YellowTaxisDeltaPartitioned/' ZORDER BY tpep_pickup_datetime WHERE PickupMonth = 12
