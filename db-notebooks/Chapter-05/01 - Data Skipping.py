# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />  
# MAGIC
# MAGIC  
# MAGIC   Name:          chapter 05/01 - Chapter 5 Optimization
# MAGIC
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 5 of the book - Performance Tuning.
# MAGIC                 This notebook illustrates data skipping statistics.
# MAGIC
# MAGIC                 
# MAGIC      The following actions are taken in this notebook:
# MAGIC        1 - Show file statistics used in data skipping
# MAGIC    

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 1 - Show file statistics used in data skipping

# COMMAND ----------

# DBTITLE 1,Show file statistics
# MAGIC %sh
# MAGIC # find the last transaction entry and search for "add"
# MAGIC # the output will show you the file stats stored in the json transaction entry for the last file added
# MAGIC grep "\"add"\" "$(ls -1rt /dbfs//mnt/datalake/book/chapter05/YellowTaxisDelta/_delta_log/*.json | tail -n1)" | sed -n 1p > /tmp/commit.json
# MAGIC python -m json.tool < /tmp/commit.json
