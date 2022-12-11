# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <span><img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC 
# MAGIC  
# MAGIC  Name:          02 - CreateDeltaTableWithSQL
# MAGIC 
# MAGIC      Author:    Bennie Haelen
# MAGIC      Date:      12-10-2022
# MAGIC      Purpose:   This notebook illustrates interactions with Hive Datbase
# MAGIC 
# MAGIC                 
# MAGIC      Commands illustrated:
# MAGIC        1 - Create Delta table with SQL
# MAGIC        2 - Create a new Hive database
# MAGIC    

# COMMAND ----------

# MAGIC %md
# MAGIC ###1 - Create an unmanaged Delta table with SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS taxidb.rateCard
# MAGIC (
# MAGIC     rateCodeId   INT,
# MAGIC     rateCodeDesc STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/datalake/book/chapter03/rateCard';

# COMMAND ----------

# MAGIC %md
# MAGIC ###2 - Show the table in the taxidb database

# COMMAND ----------

# MAGIC %sql
# MAGIC USE taxidb;
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - Run a directory listing on our table's files directory

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter03/rateCard

# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - Show the contents of the table's transaction log

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter03/rateCard/_delta_log

# COMMAND ----------

# MAGIC %md
# MAGIC ###5 - Show the **metadata** entry in the transaction log entry

# COMMAND ----------

# MAGIC %sh
# MAGIC grep metadata /dbfs/mnt/datalake/book/chapter03/rateCard/_delta_log/00000000000000000000.json > /tmp/metadata.json
# MAGIC python -m json.tool /tmp/metadata.json

# COMMAND ----------

# MAGIC %md
# MAGIC ###6 - Drop the rateCard table and re-created it as a managed table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE default.rateCard

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS taxidb.rateCard
# MAGIC (
# MAGIC     rateCodeId   INT,
# MAGIC     rateCodeDesc STRING
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ###7 - Show the directory of the managed table

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/user/hive/warehouse/taxidb.db/ratecard

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS delta.`/dluar/tables/rateCard_unmanaged`
# MAGIC (
# MAGIC     rateCodeId   INT,
# MAGIC     rateCodeDesc STRING
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ####8 - Run a DESCRIBE on the taxidb database

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE taxidb;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE taxidb;
# MAGIC DESCRIBE TABLE rateCard;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE taxidb;
# MAGIC DESCRIBE TABLE EXTENDED rateCard;

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Databricks
