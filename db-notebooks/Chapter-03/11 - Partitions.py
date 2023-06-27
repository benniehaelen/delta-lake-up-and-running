# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />   
# MAGIC
# MAGIC  Name:          chapter 03/11 - Partitions
# MAGIC  
# MAGIC      Author:    Bennie Haelen
# MAGIC      Date:      12-10-2022
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 3 of the book - Table Deletes, Updates and Merges
# MAGIC                 This notebook performs a number of Delete Operations
# MAGIC
# MAGIC                 
# MAGIC      The following Delta Lake functionality is demonstrated in this notebook:
# MAGIC        1  - Create a YellowTaxisPartitioned Delta Table that is partitioned by VendoId
# MAGIC        2  - Read the YellowTaxis data from the YellowTaxis table
# MAGIC        3  - Write the DataFrame with the YellowTaxis data to the Delta Files of the Partitioned Table
# MAGIC        4  - Take a look at the YellowTaxisDeltaPartitioned directory
# MAGIC        5  - Drop the YellowTaxisPartitioned Table and its Delta Files
# MAGIC        6  - Re-create the table, this time partitional by both VendorID and RateCodeId
# MAGIC        7  - Re-load the new table
# MAGIC        8  - Look at the top-level directory after re-partitioning, see how it is still first partitioned by VendorId
# MAGIC        9  - Look at the second level directory level where we see the folders by RateCodeId
# MAGIC        10 - Look at the third level, where we can see the Parquet files
# MAGIC        11 - Execute a SQL Query to check if a particular partition exists
# MAGIC        12 - Query the PaymentTypes for one Partition
# MAGIC        13 - Use replaceWhere to set the PaymentType for just one partition
# MAGIC        14 - Verify that the PaymentTypes have indeed been updated, but just for this partition
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###1 - Create a YellowTaxisPartitioned Delta Table that is partitioned by VendorId

# COMMAND ----------

# MAGIC %sql
# MAGIC -- If there is an ond version of the table, make sure to 
# MAGIC -- drop it
# MAGIC drop table if exists taxidb.YellowTaxisPartitioned;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the table
# MAGIC CREATE TABLE taxidb.YellowTaxisPartitioned
# MAGIC (
# MAGIC     RideId                  INT,
# MAGIC     VendorId                INT,
# MAGIC     PickupTime              TIMESTAMP,
# MAGIC     DropTime                TIMESTAMP,
# MAGIC     PickupLocationId        INT,
# MAGIC     DropLocationId          INT,
# MAGIC     CabNumber               STRING,
# MAGIC     DriverLicenseNumber     STRING,
# MAGIC     PassengerCount          INT,
# MAGIC     TripDistance            DOUBLE,
# MAGIC     RatecodeId              INT,
# MAGIC     PaymentType             INT,
# MAGIC     TotalAmount             DOUBLE,
# MAGIC     FareAmount              DOUBLE,
# MAGIC     Extra                   DOUBLE,
# MAGIC     MtaTax                  DOUBLE,
# MAGIC     TipAmount               DOUBLE,
# MAGIC     TollsAmount             DOUBLE,         
# MAGIC     ImprovementSurcharge    DOUBLE
# MAGIC     
# MAGIC ) USING DELTA         
# MAGIC PARTITIONED BY(VendorId) -- Partition by VendorId
# MAGIC LOCATION "/mnt/datalake/book/chapter03/YellowTaxisDeltaPartitioned"

# COMMAND ----------

# MAGIC %md
# MAGIC ###2 - Read the YellowTaxis data from the YellowTaxis table

# COMMAND ----------

input_df = spark.read.format("delta").table("taxidb.YellowTaxis")

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - Write the DataFrame with the YellowTaxis data to the Delta Files of the Partitioned Table

# COMMAND ----------

input_df                                                               \
    .write                                                             \
    .format("delta")                                                   \
    .mode("overwrite")                                                 \
    .save("/mnt/datalake/book/chapter03/YellowTaxisDeltaPartitioned")


# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - Take a look at the YellowTaxisDeltaPartitioned directory

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter03/YellowTaxisDeltaPartitioned

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter03/YellowTaxisDeltaPartitioned/VendorId=4

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT 
# MAGIC     DISTINCT(VendorId) 
# MAGIC FROM
# MAGIC     taxidb.YellowTaxisPartitioned;

# COMMAND ----------

# MAGIC %md
# MAGIC ###5 - Drop the YellowTaxisPartitioned table and its Delta Files

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS taxidb.yellowTaxisPartitioned

# COMMAND ----------

# MAGIC %sh
# MAGIC # Delete the Delta Files for the table
# MAGIC rm -r /dbfs/mnt/datalake/book/chapter03/YellowTaxisDeltaPartitioned

# COMMAND ----------

# MAGIC %md
# MAGIC ###6 - Re-Create the YellowTaxisPartitioned Table, this time partition by VendorId and RateCodeId

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the table
# MAGIC CREATE TABLE taxidb.YellowTaxisPartitioned
# MAGIC (
# MAGIC     RideId                  INT,
# MAGIC     VendorId                INT,
# MAGIC     PickupTime              TIMESTAMP,
# MAGIC     DropTime                TIMESTAMP,
# MAGIC     PickupLocationId        INT,
# MAGIC     DropLocationId          INT,
# MAGIC     CabNumber               STRING,
# MAGIC     DriverLicenseNumber     STRING,
# MAGIC     PassengerCount          INT,
# MAGIC     TripDistance            DOUBLE,
# MAGIC     RatecodeId              INT,
# MAGIC     PaymentType             INT,
# MAGIC     TotalAmount             DOUBLE,
# MAGIC     FareAmount              DOUBLE,
# MAGIC     Extra                   DOUBLE,
# MAGIC     MtaTax                  DOUBLE,
# MAGIC     TipAmount               DOUBLE,
# MAGIC     TollsAmount             DOUBLE,         
# MAGIC     ImprovementSurcharge    DOUBLE
# MAGIC     
# MAGIC ) USING DELTA         
# MAGIC PARTITIONED BY(VendorId, RatecodeId) -- Partition by VendorId AND rateCodeId
# MAGIC LOCATION "/mnt/datalake/book/chapter03/YellowTaxisDeltaPartitioned"

# COMMAND ----------

# MAGIC %md
# MAGIC ###7 - Re-load the new table

# COMMAND ----------

input_df = spark.read.format("delta").table("taxidb.YellowTaxis")

# COMMAND ----------

input_df                                                               \
    .write                                                             \
    .format("delta")                                                   \
    .mode("overwrite")                                                 \
    .save("/mnt/datalake/book/chapter03/YellowTaxisDeltaPartitioned")

# COMMAND ----------

# MAGIC %md
# MAGIC ###8 - Take another look at the directory, see how it is still first partitioned by VendorId

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter03/YellowTaxisDeltaPartitioned

# COMMAND ----------

# MAGIC %md
# MAGIC ###9 - At the second directory level we see the RateCodeId

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter03/YellowTaxisDeltaPartitioned/VendorId=1

# COMMAND ----------

# MAGIC %md
# MAGIC ###10 - Finally, at the third level we have our Parquet files

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter03/YellowTaxisDeltaPartitioned/VendorId=1/RatecodeId=1

# COMMAND ----------

# MAGIC %md
# MAGIC ###11 - A SQL Query to help you find out if a partition exists

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COUNT(*) > 0 AS `Partition exists`
# MAGIC FROM 
# MAGIC     taxidb.YellowTaxisPartitioned
# MAGIC WHERE 
# MAGIC     VendorId = 2 AND RateCodeId = 99

# COMMAND ----------

# MAGIC %md
# MAGIC ###12 - Query the PaymentTypes for one Partition

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  
# MAGIC     RideId, VendorId, PaymentType
# MAGIC FROM    
# MAGIC     taxidb.yellowtaxispartitioned
# MAGIC WHERE   
# MAGIC     VendorID = 1 AND RatecodeId = 99
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###13 - Use replaceWhere to set the PaymentType for just one partition

# COMMAND ----------

from pyspark.sql.functions import *

spark.read                                                              \
    .format("delta")                                                    \
    .load("/mnt/datalake/book/chapter03/YellowTaxisDeltaPartitioned")   \
    .where((col("VendorId") == 1) & (col("RatecodeId") == 99))          \
    .withColumn("PaymentType", lit(3))                                  \
    .write                                                              \
    .format("delta")                                                    \
    .option("replaceWhere", "VendorId = 1 AND RateCodeId = 99")         \
    .mode("overwrite")                                                  \
    .save("/mnt/datalake/book/chapter03/YellowTaxisDeltaPartitioned")

# COMMAND ----------

# MAGIC %md
# MAGIC ###14 - Verify that the PaymentTypes have indeed been updated, but just for this partition

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  
# MAGIC     DISTINCT(PaymentType)
# MAGIC FROM    
# MAGIC     taxidb.yellowtaxispartitioned
# MAGIC WHERE   
# MAGIC     VendorID = 1 AND RatecodeId = 99

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  
# MAGIC     DISTINCT(PaymentType)
# MAGIC FROM    
# MAGIC     taxidb.yellowtaxispartitioned
# MAGIC ORDER BY 
# MAGIC     PaymentType
