# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <span><img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC 
# MAGIC  
# MAGIC  Name:          chapter 03/03 - The Describe Command
# MAGIC 
# MAGIC      Author:    Bennie Haelen
# MAGIC      Date:      12-10-2022
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 3 of the book - Basic Operations on Delta Tables.
# MAGIC                 This notebook illustrates how to use the SQL DESCRIBE command with both Hive databases and tables
# MAGIC 
# MAGIC                 
# MAGIC      The following Delta Lake functionality is demonstrated in this notebook:
# MAGIC        1 - Running the DESCRIBE command on a Hive Database
# MAGIC        2 - Running the DESCRIBE command on a unmanaged Delta Table
# MAGIC        3 - Running the DESCRIBE EXTENDED command on a unmanaged Delta Table
# MAGIC        4 - Running the DESCRIBE EXTENDED command on a managed Delta Table
# MAGIC    

# COMMAND ----------

# MAGIC %md
# MAGIC ###1 - Run the DESCRIBE command on a Hive database

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Running DESCRIBE on a database returns the namespace (aka teh database name), and comments
# MAGIC -- entered while createing the database, the location for unmanaged tables in the database
# MAGIC -- and the owner of the database
# MAGIC DESCRIBE DATABASE taxidb;

# COMMAND ----------

# MAGIC %md
# MAGIC ###2 - Run the DESCRIBE command on a Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Running the DESCRIBE command on a table returns the columns names 
# MAGIC -- and Data Types, together with the partitioning information.
# MAGIC DESCRIBE TABLE taxidb.rateCard;

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - Running the DESCRIBE EXTENDED command on a unmanaged table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Running the DESCRIBE EXTENDED command returns additional metadatata
# MAGIC -- such as the Hive Database name, the location of the underlying Delta
# MAGIC -- files and the table properties
# MAGIC DESCRIBE TABLE EXTENDED taxidb.rateCard;

# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - Running the DESCRIBE EXTENDED command on a managed table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- When we run the DESCRIBE EXTENDED command on a manageed table, we see
# MAGIC -- that the files for the table are located under the /user/hive/warehouse
# MAGIC -- directory
# MAGIC DESCRIBE TABLE EXTENDED taxidb.rateCardManaged
