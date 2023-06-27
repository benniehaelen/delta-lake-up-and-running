# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />  
# MAGIC
# MAGIC  
# MAGIC Name:          chapter 04/01 - Delete Operations
# MAGIC
# MAGIC      Author:    Bennie Haelen
# MAGIC      Date:      12-10-2022
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 4 of the book - Basic Operations on Delta Tables.
# MAGIC                 This notebook executes a DELETE operation and shows the impact on the Parquet part files and the details
# MAGIC                 of what is happening in the transaction log.
# MAGIC
# MAGIC                 
# MAGIC      The following actions are taken in this notebook:
# MAGIC        1 - Perform a DESCRIBE HISTORY on the Starting Table
# MAGIC        2 - Demonstrate that we currently have one transaction log entry
# MAGIC        3 - Get the first "add file" action from the transaction log entry
# MAGIC        4 - Get the second "add file" action from the transaction log entry
# MAGIC        5 - Confirm the part files with a directory listing
# MAGIC        6 - Perform a delete on a single row
# MAGIC        7 - Use DESCRIBE HISTORY to look at the DELETE operation
# MAGIC        8 - Search the transaction log entry for the "Add File" and "Remove File" actions
# MAGIC        9 - Perform a directory listing to confirm the parquet part files
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
# MAGIC ###2 - Demonstrate that we currently have one transaction log entry

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/mnt/datalake/book/chapter04/YellowTaxisDelta/_delta_log/*.json

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - Get the first "add file" action from the transaction log entry

# COMMAND ----------

# MAGIC %sh
# MAGIC grep \"add\" /dbfs/mnt/datalake/book/chapter04/YellowTaxisDelta/_delta_log/00000000000000000000.json | sed -n 1p > /tmp/commit.json
# MAGIC python -m json.tool < /tmp/commit.json

# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - Get the second "add file" action from the transaction log entry

# COMMAND ----------

# MAGIC %sh
# MAGIC grep "add" /dbfs/mnt/datalake/book/chapter04/YellowTaxisDelta/_delta_log/00000000000000000000.json | sed -n 2p > /tmp/commit.json
# MAGIC python -m json.tool < /tmp/commit.json

# COMMAND ----------

# MAGIC %md 
# MAGIC ###5 - Confirm the part files with a directory listing

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter04/YellowTaxisDelta

# COMMAND ----------

# MAGIC %md
# MAGIC ###6 - Perform a delete on a single row

# COMMAND ----------

# MAGIC %sql
# MAGIC -- First, show that we have data for RideId = 999998
# MAGIC SELECT  
# MAGIC     RideId, 
# MAGIC     VendorId, 
# MAGIC     CabNumber, 
# MAGIC     TotalAmount
# MAGIC FROM    
# MAGIC     taxidb.YellowTaxis
# MAGIC WHERE   
# MAGIC     RideId = 100000
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Perform the actual delete
# MAGIC DELETE FROM
# MAGIC     taxidb.YellowTaxis
# MAGIC WHERE RideId = 100000

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Make sure that the row with  RideId = 999998 is really gone
# MAGIC SELECT  
# MAGIC     RideId, 
# MAGIC     VendorId, 
# MAGIC     CabNumber, 
# MAGIC     TotalAmount
# MAGIC FROM    
# MAGIC     taxidb.YellowTaxis
# MAGIC WHERE   
# MAGIC     RideId = 100000

# COMMAND ----------

# MAGIC %md
# MAGIC ###7 - Use DESCRIBE HISTORY to look at the DELETE operation

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY taxidb.YellowTaxis

# COMMAND ----------

# MAGIC %md
# MAGIC ###8 - Search the transaction log entry for the "Add File" and "Remove File" actions

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/mnt/datalake/book/chapter04/YellowTaxisDelta/_delta_log/*.json

# COMMAND ----------

# MAGIC %sh
# MAGIC grep "add" /dbfs/mnt/datalake/book/chapter04/YellowTaxisDelta/_delta_log/00000000000000000001.json > /tmp/commit.json
# MAGIC python -m json.tool < /tmp/commit.json
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC grep "remove" /dbfs/mnt/datalake/book/chapter04/YellowTaxisDelta/_delta_log/00000000000000000001.json > /tmp/commit.json
# MAGIC python -m json.tool < /tmp/commit.json

# COMMAND ----------

# MAGIC %md
# MAGIC ###9 - Perform a directory listing to confirm the parquet part files

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter04/YellowTaxisDelta/
# MAGIC
