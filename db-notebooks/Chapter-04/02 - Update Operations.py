# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />  
# MAGIC
# MAGIC  
# MAGIC Name:          chapter 04/02 - Update Operations
# MAGIC
# MAGIC      Author:    Bennie Haelen
# MAGIC      Date:      12-10-2022
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 4 of the book - Basic Operations on Delta Tables.
# MAGIC                 This notebook perform an UPDATE operation and shows the impact on the part files and the details of what
# MAGIC                 is recorded in the transaction log.
# MAGIC
# MAGIC                 
# MAGIC      The following actions are taken in this notebook:
# MAGIC        1 - Perform a DESCRIBE HISTORY on the Starting Table
# MAGIC        2 - Perform a SELECT on the RideId to confirm that our record exists
# MAGIC        3 - Perform the UPDATE
# MAGIC        4 - Verify that the record has been updated correctly
# MAGIC        5 - Use DESCRIBE HISTORY to look at the UPDATE operation
# MAGIC        6 - Search the transaction log entry for the "Add File" and "Remove File" actions
# MAGIC
# MAGIC    

# COMMAND ----------

# MAGIC %md
# MAGIC ###1 - Perform a DESCRIBE HISTORY on the Starting Table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY taxidb.YellowTaxis

# COMMAND ----------

# MAGIC %md
# MAGIC ###2 - Perform a SELECT on the RideId to confirm that our record exists

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     INPUT_FILE_NAME(),
# MAGIC     RideId,
# MAGIC     VendorId,
# MAGIC     DropLocationId
# MAGIC FROM    
# MAGIC     taxidb.YellowTaxis
# MAGIC WHERE 
# MAGIC     RideId = 9999994
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - Perform the UPDATE

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE 
# MAGIC     taxidb.YellowTaxis
# MAGIC SET 
# MAGIC     DropLocationId = 250
# MAGIC WHERE 
# MAGIC     RideId = 9999994

# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - Verify that the record has been updated correctly

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  
# MAGIC     RideId,
# MAGIC     DropLocationId
# MAGIC FROM 
# MAGIC     taxidb.YellowTaxis
# MAGIC WHERE 
# MAGIC     RideId = 9999994
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###5 - Use DESCRIBE HISTORY to look at the UPDATE operation

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY taxidb.YellowTaxis

# COMMAND ----------

# MAGIC %md
# MAGIC ###6 - Search the transaction log entry for the "Add File" and "Remove File" actions

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter04/YellowTaxisDelta/*.parquet

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter04/YellowTaxisDelta/_delta_log/*.json

# COMMAND ----------

# MAGIC %sh
# MAGIC grep "add" /dbfs/mnt/datalake/book/chapter04/YellowTaxisDelta/_delta_log/00000000000000000002.json > /tmp/commit.json
# MAGIC python -m json.tool < /tmp/commit.json

# COMMAND ----------

# MAGIC %sh
# MAGIC grep "remove" /dbfs/mnt/datalake/book/chapter04/YellowTaxisDelta/_delta_log/00000000000000000002.json > /tmp/commit.json
# MAGIC python -m json.tool < /tmp/commit.json
