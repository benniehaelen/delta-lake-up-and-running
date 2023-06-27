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
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 7 of the book - Updating and Modifying Table Schema
# MAGIC                 This notebook covers explicit schema updates
# MAGIC
# MAGIC                 
# MAGIC      The following actions are taken in this notebook:
# MAGIC        1 - Drop the taxidb database with a cascade, deleting all tables in the database
# MAGIC        2 - ...
# MAGIC    

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1 - Reset the TaxiRateCode Delta Table

# COMMAND ----------

# Make sure to import the StructType and all supporting
# cast of Type classes (StringType, IntegerType etc..)
from pyspark.sql.types import *

from pyspark.sql.functions import col, cast

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/datalake/book/chapter07/TaxiRateCode.delta

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table taxidb.taxiratecode

# COMMAND ----------

# Read the .CSV file, make sure that we use an integer
# for the RateCodeId
df = spark.read.format("csv")      \
        .option("header", "true") \
        .load("/mnt/datalake/book/chapter07/TaxiRateCode.csv")
df = df.withColumn("RateCodeId", df["RateCodeId"].cast(IntegerType()))

# Write in Delta Lake format
df.write.format("delta")   \
        .mode("overwrite") \
        .save("/mnt/datalake/book/chapter07/TaxiRateCode.delta")
    
# print the schema
df.printSchema()

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Replace the TaxiRateCode table
# MAGIC CREATE TABLE taxidb.TaxiRateCode
# MAGIC (
# MAGIC     RateCodeId              INT,
# MAGIC     RateCodeDesc            STRING
# MAGIC     
# MAGIC ) USING DELTA         
# MAGIC LOCATION "/mnt/datalake/book/chapter07/TaxiRateCode.delta";
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Perform a SELECT, verify that we are indeed starting with our default 
# MAGIC -- 6 records
# MAGIC select * from taxidb.TaxiRateCode

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2 - Add Column (Use Case 1)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add a new column after the RateCodeId
# MAGIC ALTER TABLE delta.`/mnt/datalake/book/chapter07/TaxiRateCode.delta`
# MAGIC ADD COLUMN RateCodeTaxPercent INT AFTER RateCodeId

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE delta.`/mnt/datalake/book/chapter07/TaxiRateCode.delta`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show that the values for the new columns are NULL, as expected
# MAGIC select * from delta.`/mnt/datalake/book/chapter07/TaxiRateCode.delta`

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs//mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/*.json

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/00000000000000000001.json 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe HISTORY taxidb.taxiratecode

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3 - Add a Comment to a Column (Use Case 2)

# COMMAND ----------

# MAGIC %sql
# MAGIC --
# MAGIC -- Add a comment to the RateCodeId column
# MAGIC --
# MAGIC ALTER TABLE taxidb.TaxiRateCode
# MAGIC ALTER COLUMN RateCodeId COMMENT 'This is the id of the Ride'

# COMMAND ----------

# MAGIC %sh
# MAGIC grep "commitInfo" /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/00000000000000000001.json > /tmp/commit.json
# MAGIC python -m json.tool < /tmp/commit.json

# COMMAND ----------

# MAGIC %sh
# MAGIC grep "metaData" /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/00000000000000000001.json > /tmp/commit.json
# MAGIC python -m json.tool < /tmp/commit.json

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY taxidb.TaxiRateCode 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM taxidb.TaxiRateCode

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4 - Reorder Columns (Use Case 3)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE taxidb.TaxiRateCode 

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE taxidb.TaxiRateCode  ALTER COLUMN RateCodeDesc AFTER RateCodeId

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED taxidb.TaxiRateCode 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM taxidb.TaxiRateCode

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/*.json

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/00000000000000000003.json

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 5 - Setup Column Mapping

# COMMAND ----------

# MAGIC  %sql
# MAGIC  ALTER TABLE taxidb.TaxiRateCode  SET TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = '2',
# MAGIC     'delta.minWriterVersion' = '5',
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC   )

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/*.json

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/00000000000000000004.json

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM taxidb.TaxiRateCode

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED taxidb.TaxiRateCode 

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 6 - Rename a Column (Use case 4)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Perform our column rename
# MAGIC ALTER TABLE taxidb.taxiratecode RENAME COLUMN RateCodeDesc to RateCodeDescription

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/*.json

# COMMAND ----------

# MAGIC %sh
# MAGIC # Look at the corresponding log entry
# MAGIC cat /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/00000000000000000005.json

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 7 - REPLACE COLUMNS (Use Case 5)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE taxidb.TaxiRateCode 
# MAGIC REPLACE COLUMNS (
# MAGIC   Rate_Code_Identifier  INT    COMMENT 'Identifies the code',
# MAGIC   Rate_Code_Description STRING COMMENT 'Describes the code',
# MAGIC   Rate_Code_Percentage  INT    COMMENT 'Tax percentage applied'
# MAGIC )

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/*.json

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/00000000000000000006.json
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM taxidb.TaxiRateCode 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY taxidb.TaxiRateCode 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxidb.taxiratecode

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED taxidb.TaxiRateCode 

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/00000000000000000000.json

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Notice that all columns will be NULL after
# MAGIC -- the REPLACE COLUMNS operation
# MAGIC SELECT * FROM taxidb.TaxiRateCode

# COMMAND ----------

# MAGIC %md
# MAGIC ###Case 6 - Drop Column

# COMMAND ----------

# MAGIC %md
# MAGIC ####First, we need to cleanup and restore our original table

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/datalake/book/chapter07/TaxiRateCode.delta

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS taxidb.TaxiRateCode

# COMMAND ----------

df = spark.read.format("csv")      \
        .option("header", "true") \
        .load("/mnt/datalake/book/chapter07/TaxiRateCode.csv")
df = df.withColumn("RateCodeId", df["RateCodeId"].cast(IntegerType()))

# Write in Delta Lake format
df.write.format("delta")   \
        .mode("overwrite") \
        .save("/mnt/datalake/book/chapter07/TaxiRateCode.delta")
    
# print the schema
df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Re-create YellowTaxis as an unmanaged table
# MAGIC CREATE TABLE taxidb.TaxiRateCode
# MAGIC (
# MAGIC     RateCodeId              INT,
# MAGIC     RateCodeDesc            STRING
# MAGIC     
# MAGIC ) USING DELTA         
# MAGIC LOCATION "/mnt/datalake/book/chapter07/TaxiRateCode.delta";

# COMMAND ----------

# MAGIC  %sql
# MAGIC  ALTER TABLE taxidb.TaxiRateCode  SET TBLPROPERTIES (
# MAGIC     'delta.minReaderVersion' = '2',
# MAGIC     'delta.minWriterVersion' = '5',
# MAGIC     'delta.columnMapping.mode' = 'name'
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC ####Drop the RateCodeDesc column

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use the ALTER TABLE... DROP COLUMN command
# MAGIC -- to drop the RateCodeDesc column
# MAGIC ALTER TABLE taxidb.TaxiRateCode DROP COLUMN RateCodeDesc

# COMMAND ----------

# MAGIC %sql
# MAGIC -- When we look at the schema, we see that our column has indeed been dropped
# MAGIC DESCRIBE EXTENDED taxidb.TaxiRateCode

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Select the remaining columns
# MAGIC SELECT * FROM taxidb.TaxiRateCode

# COMMAND ----------

# MAGIC %sh
# MAGIC # When you take a look at the commitInfo, you can see the DROP COLUMNS operation
# MAGIC # cat /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/00000000000000000002.json
# MAGIC grep "commitInfo" /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/00000000000000000002.json > /tmp/commit.json
# MAGIC python -m json.tool < /tmp/commit.json

# COMMAND ----------

# MAGIC %sh
# MAGIC # When you take a look at the metadata, we see the new schemaString, and the metadata mapping
# MAGIC grep "metaData" /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/00000000000000000002.json > /tmp/commit.json
# MAGIC python -m json.tool < /tmp/commit.json

# COMMAND ----------

# MAGIC %sh
# MAGIC # Display the data file(s)
# MAGIC # We can see we only have our one part file, which was not 
# MAGIC # touched at all
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reorganize the table by removing the part file which included 
# MAGIC -- the RateCodeDesc column and adding a new part file with just the 
# MAGIC -- RateCodeId column
# MAGIC REORG TABLE taxidb.TaxiRateCode APPLY (PURGE)

# COMMAND ----------

# MAGIC %sh
# MAGIC # Look at the Transaction Log entry for the REORG Table Command
# MAGIC cat /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/00000000000000000003.json

# COMMAND ----------

# MAGIC %sh
# MAGIC # In our main directory, we have the original part file,
# MAGIC # which now has been removed from the Delta File
# MAGIC # We also notice the 9g sub-directory
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/

# COMMAND ----------

# MAGIC %sh
# MAGIC # This is the new part file, which contains just the RateCodeId column
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/9g

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VACUUM the table to remove the
# MAGIC -- obsolete part file
# MAGIC VACUUM taxidb.TaxiRateCode

# COMMAND ----------

# MAGIC %md
# MAGIC ###Case 7 - Change Column Data Type with Schema Overwrite

# COMMAND ----------

# MAGIC %md
# MAGIC ####First, we need to cleanup and restore our original table

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/datalake/book/chapter07/TaxiRateCode.delta

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS taxidb.TaxiRateCode

# COMMAND ----------

df = spark.read.format("csv")      \
        .option("header", "true") \
        .load("/mnt/datalake/book/chapter07/TaxiRateCode.csv")
df = df.withColumn("RateCodeId", df["RateCodeId"].cast(IntegerType()))

# Write in Delta Lake format
df.write.format("delta")   \
        .mode("overwrite") \
        .save("/mnt/datalake/book/chapter07/TaxiRateCode.delta")
    
# print the schema
df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Re-create YellowTaxis as an unmanaged table
# MAGIC CREATE TABLE taxidb.TaxiRateCode
# MAGIC (
# MAGIC     RateCodeId              INT,
# MAGIC     RateCodeDesc            STRING
# MAGIC     
# MAGIC ) USING DELTA         
# MAGIC LOCATION "/mnt/datalake/book/chapter07/TaxiRateCode.delta";

# COMMAND ----------

# MAGIC %md
# MAGIC ####Use overwriteSchema to re-write the table

# COMMAND ----------

#
# Re-write the table with the overwriteSchema setting
# Use .withColumn to change the data type of the RateCodeId column
#
spark.read.table('taxidb.TaxiRateCode')                             \
        .withColumn("RateCodeId", col("RateCodeId").cast("short"))  \
        .write                                                      \
        .mode("overwrite")                                          \
        .option("overwriteSchema", "true")                          \
        .saveAsTable('taxidb.TaxiRateCode')

# COMMAND ----------

# MAGIC %md
# MAGIC ####Perform a battery of checks on the re-written table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Perform a describe on the table
# MAGIC -- You will now see that the RateCodeId column has
# MAGIC -- the smallint or short data type
# MAGIC DESCRIBE taxidb.TaxiRateCode

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Here, we make sure that all of our
# MAGIC -- data is still there, and we can confirm
# MAGIC -- it indeed is still there
# MAGIC SELECT
# MAGIC     *
# MAGIC FROM  
# MAGIC     taxidb.TaxiRateCode

# COMMAND ----------

# MAGIC %md
# MAGIC ####Verify by checking the transaction log entries

# COMMAND ----------

# MAGIC %sh
# MAGIC # We should have two transaction log entries:
# MAGIC #  1. The first one from the table creation
# MAGIC #  2. The second one from our table re-write. This is the one that we 
# MAGIC #     are interested in
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log

# COMMAND ----------

# MAGIC %sh
# MAGIC # First, let's have a look at the commitInfo entry.
# MAGIC # We see that the OPERATION is listed as CREATE OR REPLACE TABLE AS SELECT
# MAGIC grep "commitInfo" /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/00000000000000000001.json > /tmp/commit.json
# MAGIC python -m json.tool < /tmp/commit.json

# COMMAND ----------

# MAGIC %sh
# MAGIC # The metadata field contains our expected schemaString
# MAGIC grep "metaData" /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/00000000000000000001.json > /tmp/commit.json
# MAGIC python -m json.tool < /tmp/commit.json

# COMMAND ----------

# MAGIC %sh
# MAGIC # We have one remove entry, which removes our old part file
# MAGIC grep "remove" /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/00000000000000000001.json > /tmp/commit.json
# MAGIC python -m json.tool < /tmp/commit.json

# COMMAND ----------

# MAGIC %sh
# MAGIC # We have one add entry, which adds our part file with our 6 records
# MAGIC grep '"add"' /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/00000000000000000001.json > /tmp/commit.json
# MAGIC python -m json.tool < /tmp/commit.json
