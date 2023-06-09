# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />  
# MAGIC
# MAGIC  
# MAGIC   Name:          chapter 08/ 03 - Change Data Feed
# MAGIC
# MAGIC      Author:    Bennie Haelen
# MAGIC      Date:      12-10-2022
# MAGIC      Purpose:   The notebooks in this folder contains the code for the "Change Data Feed" 
# MAGIC                 example for chapter 8 of the book - Delta Lake Streaming
# MAGIC
# MAGIC                 
# MAGIC      The following actions are taken in this notebook:
# MAGIC        1 - Create the cdf database
# MAGIC        2 - Create the DimSalesRep table with CDF enabled
# MAGIC        3 - Insert a number of records. Does this create CDF data?
# MAGIC        4 - Perform an SCD type 2 - style Update
# MAGIC        5 - Look at this impact of the MERGE statement
# MAGIC        6 - Use table_changes() to look at CDC information
# MAGIC        7 - Streaming Table Changes
# MAGIC    

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create the cdf database

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists cdf cascade

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS cdf

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -r /dbfs/mnt/datalake/book/chapter08/cdf/DimSalesRep

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2 - Create the DimSalesRep table with CDF enabled

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE cdf.DimSalesRep
# MAGIC (
# MAGIC   salesRepId    INT,
# MAGIC   name          STRING, 
# MAGIC   salesRegion   STRING,
# MAGIC   isCurrent     BOOLEAN,
# MAGIC   isEmployed    BOOLEAN,
# MAGIC   effectiveDate STRING,
# MAGIC   endDate       STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/datalake/book/chapter08/cdf/DimSalesRep'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = True)
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter08/cdf/DimSalesRep

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3 - Insert a number of records. Does this create CDF data?

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO 
# MAGIC   cdf.dimsalesrep
# MAGIC     (salesRepId, name, salesRegion, isCurrent, isEmployed, effectiveDate, endDate)
# MAGIC VALUES
# MAGIC     (1,  "Joe Salesman", "East", True, True, "2020-02-03", null),
# MAGIC     (11, "Seattle Sue", "Northwest", True, True, "2019-12-03", null),
# MAGIC     (21, "California Dreaming", "Southwest", True, True, "2021-05-23", null);
# MAGIC
# MAGIC SELECT * FROM cdf.DimSalesRep;
# MAGIC        
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC # You will notice that inserts do not generate anything new
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter08/cdf/DimSalesRep

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4 - Perform an SCD type 2 - style Update

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW myUpdates
# MAGIC AS
# MAGIC SELECT 1 AS salesRepId, "Joe Salesman" as name, "NorthEast" AS salesRegion, 
# MAGIC        true AS isCurrent, true AS isEmployed, 
# MAGIC        "2023-4-17" AS effectiveDate, null AS endDate
# MAGIC UNION
# MAGIC
# MAGIC
# MAGIC SELECT 51 as salesRepId, "Winner Man" as name, "SouthEast" as salesRegion,
# MAGIC        true AS isCurrent, true AS isEmployed, 
# MAGIC        "2023-4-18" AS effectiveDate, null AS endDate
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO cdf.DimSalesRep as source
# MAGIC USING (
# MAGIC   SELECT myUpdates.salesRepId as mergeKey, myUpdates.*
# MAGIC   FROM myUpdates
# MAGIC
# MAGIC   UNION ALL 
# MAGIC
# MAGIC   SELECT null as mergeKey, myUpdates.*
# MAGIC   FROM myUpdates INNER JOIN
# MAGIC       cdf.DimSalesRep source on myUpdates.salesRepId = source.salesRepId
# MAGIC   WHERE 
# MAGIC       source.isCurrent = true
# MAGIC ) updateMerges
# MAGIC ON source.salesRepId = updateMerges.mergeKey
# MAGIC WHEN MATCHED AND source.isCurrent = true THEN
# MAGIC   UPDATE SET isCurrent = false, endDate = updateMerges.effectiveDate
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT 
# MAGIC      (salesRepId, name, salesRegion, isCurrent, isEmployed, effectiveDate, endDate)
# MAGIC     VALUES(updateMerges.salesRepId, 
# MAGIC            updateMerges.name,
# MAGIC            updateMerges.salesRegion, 
# MAGIC            updateMerges.isCurrent, 
# MAGIC            updateMerges.isEmployed, 
# MAGIC            updateMerges.effectiveDate, 
# MAGIC            updateMerges.endDate)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cdf.dimsalesrep order by salesRepId, isCurrent DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 5 - Look at this impact of the MERGE statement

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter08/cdf/DimSalesRep

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter08/cdf/DimSalesRep/_change_data

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter08/cdf/DimSalesRep/_delta_log/*.json

# COMMAND ----------

# MAGIC %sh
# MAGIC head /dbfs/mnt/datalake/book/chapter08/cdf/DimSalesRep/_delta_log/00000000000000000002.json

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY  cdf.dimsalesrep

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir /dbfs/mnt/datalake/book/chapter08/CDCEval/

# COMMAND ----------

# MAGIC %sh
# MAGIC cp /dbfs/mnt/datalake/book/chapter08/cdf/DimSalesRep/_change_data/cdc*.parquet /dbfs/mnt/datalake/book/chapter08/CDCEval/

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter08/CDCEval/

# COMMAND ----------

cdc_df1 = spark.read.parquet("/mnt/datalake/book/chapter08/CDCEval/cdc-00000-53fd9a20-f1aa-44cf-8992-ef6ee3c77b7c.c000.snappy.parquet")
cdc_df2 = spark.read.parquet("/mnt/datalake/book/chapter08/CDCEval/cdc-00001-3d4e2227-0730-4a70-bdfc-ade83a1bad95.c000.snappy.parquet")
cdc_df = cdc_df1.union(cdc_df2)
cdc_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 6 - Use table_changes() to look at CDC information

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM table_changes('cdf.dimsalesrep', 0, 2)
# MAGIC WHERE _change_type != 'update_preimage' and salesRepId = 1
# MAGIC ORDER BY _commit_timestamp

# COMMAND ----------

# MAGIC  %md
# MAGIC  ###Step 7 - Streaming Table Changes

# COMMAND ----------

# Read the CDC stream with the "readChangeFeed" option
df = spark.readStream                        \
          .format("delta")                   \
          .option("readChangeFeed", "true")  \
          .option("startingVersion", 0)      \
          .table("cdf.dimSalesRep")

df.display()
