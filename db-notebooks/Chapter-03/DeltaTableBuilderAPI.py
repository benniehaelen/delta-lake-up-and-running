# Databricks notebook source
from delta.tables import *

# COMMAND ----------

# Create table in the metastore
DeltaTable.createIfNotExists(spark) \
  .tableName("default.people10m") \
  .addColumn("id", "INT") \
  .addColumn("firstName", "STRING") \
  .addColumn("middleName", "STRING") \
  .addColumn("lastName", "STRING", comment = "surname") \
  .addColumn("gender", "STRING") \
  .addColumn("birthDate", "TIMESTAMP") \
  .addColumn("ssn", "STRING") \
  .addColumn("salary", "INT") \
  .execute()

# Create or replace table with path and add properties
DeltaTable.createOrReplace(spark) \
  .addColumn("id", "INT") \
  .addColumn("firstName", "STRING") \
  .addColumn("middleName", "STRING") \
  .addColumn("lastName", "STRING", comment = "surname") \
  .addColumn("gender", "STRING") \
  .addColumn("birthDate", "TIMESTAMP") \
  .addColumn("ssn", "STRING") \
  .addColumn("salary", "INT") \
  .property("description", "table with people data") \
  .location("/tmp/delta/people10m") \
  .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ###1. Create a managed table using the DeltaTableBuilder API

# COMMAND ----------

# DBTITLE 1,Create the greentaxis table as an unmanaged table
#
# In this Create Table, we do NOT specify a location, so we are 
# creating a MANAGED table
#
DeltaTable.createIfNotExists(spark)                              \
    .tableName("taxidb.greentaxis")                             \
    .addColumn("RideId", "INT", comment = "Primary Key")         \
    .addColumn("VendorId", "INT", comment = "Ride Vendor")       \
    .addColumn("EventType", "STRING")                            \
    .addColumn("PickupTime", "TIMESTAMP")                        \
    .addColumn("PickupLocationId", "INT")                        \
    .addColumn("CabLicense", "STRING")                           \
    .addColumn("DriversLicense", "STRING")                       \
    .addColumn("PassengerCount", "INT")                          \
    .addColumn("DropTime", "TIMESTAMP")                          \
    .addColumn("DropLocationId", "INT")                          \
    .addColumn("RateCodeId", "INT", comment = "Ref to RateCard") \
    .addColumn("PaymentType", "INT")                             \
    .addColumn("TripDistance", "DOUBLE")                         \
    .addColumn("TotalAmount", "DOUBLE")                          \
    .execute()

# COMMAND ----------

# DBTITLE 1,Detailed look at the created table
# MAGIC %sql
# MAGIC --
# MAGIC -- Let's take a look at the table
# MAGIC --
# MAGIC DESCRIBE TABLE EXTENDED default.greentaxis 

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists default.greentaxis

# COMMAND ----------

# MAGIC %md
# MAGIC ###2. Create an unmanaged table using the DeltaTableBuilder API

# COMMAND ----------

# DBTITLE 1,Create the greentaxis table as an unmanaged table
#
# In this Create Table, we do specify a location, so we are 
# creating a UNMANAGED table
#
DeltaTable.createIfNotExists(spark)                              \
    .tableName("taxidb.greentaxis")                              \
    .addColumn("RideId", "INT", comment = "Primary Key")         \
    .addColumn("VendorId", "INT", comment = "Ride Vendor")       \
    .addColumn("EventType", "STRING")                            \
    .addColumn("PickupTime", "TIMESTAMP")                        \
    .addColumn("PickupLocationId", "INT")                        \
    .addColumn("CabLicense", "STRING")                           \
    .addColumn("DriversLicense", "STRING")                       \
    .addColumn("PassengerCount", "INT")                          \
    .addColumn("DropTime", "TIMESTAMP")                          \
    .addColumn("DropLocationId", "INT")                          \
    .addColumn("RateCodeId", "INT", comment = "Ref to RateCard") \
    .addColumn("PaymentType", "INT")                             \
    .addColumn("TripDistance", "DOUBLE")                         \
    .addColumn("TotalAmount", "DOUBLE")                          \
    .property("description", "table with Green Taxi Data")       \
    .location("/dluar/ch03/greentaxi")                           \
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED default.greentaxis
