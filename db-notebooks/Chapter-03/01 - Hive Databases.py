# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <span><img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC 
# MAGIC  
# MAGIC  Name:          chapter 03/01 - Hive Databases
# MAGIC 
# MAGIC      Author:    Bennie Haelen
# MAGIC      Date:      12-10-2022
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 3 of the book - Basic Operations on Delta Tables.
# MAGIC                 This notebook illustrates interactions with the Hive Metastore
# MAGIC 
# MAGIC                 
# MAGIC      Commands illustrated:
# MAGIC        1 - Show all Hive databases
# MAGIC        2 - Create a new Hive database
# MAGIC    

# COMMAND ----------

# MAGIC %md
# MAGIC ###1 - List all Hive databases

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %md
# MAGIC ###2 - Create the taxidb database

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists taxidb;
# MAGIC show databases;
