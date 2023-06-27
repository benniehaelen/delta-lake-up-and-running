# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />   
# MAGIC
# MAGIC  Name:          chapter 03/12 - User Defined Metadata
# MAGIC  
# MAGIC      Author:    Bennie Haelen
# MAGIC      Date:      12-10-2022
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 3 of the book - Basic Operations on Delta Tables.
# MAGIC                 This notebook illustrates how to leverage User Defined Metadata
# MAGIC                 
# MAGIC      The following Delta Lake functionality is demonstrated in this notebook:
# MAGIC        1  -  Set the custom-metadata option and insert a row of data
# MAGIC        2  - ....
# MAGIC

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###1 - Set the custom-metadata option and insert a row of data

# COMMAND ----------

spark.sparkContext

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.commitInfo.userMetadata=my-custom-metadata= { "GDPR": "INSERT Request 1x965383" };
# MAGIC
# MAGIC INSERT INTO taxidb.yellowtaxisPartitioned
# MAGIC (RideId, VendorId, PickupTime, DropTime, 
# MAGIC  PickupLocationId, DropLocationId, CabNumber, 
# MAGIC  DriverLicenseNumber, PassengerCount, TripDistance, 
# MAGIC  RatecodeId, PaymentType, TotalAmount,
# MAGIC  FareAmount, Extra, MtaTax, TipAmount, 
# MAGIC  TollsAmount, ImprovementSurcharge)
# MAGIC
# MAGIC  VALUES(10000000, 3, '2019-11-01T00:00:00.000Z', 
# MAGIC         '2019-11-01T00:02:23.573Z', 65, 71, 'TAC304',
# MAGIC         '453987', 2, 4.5, 1, 1, 20.34, 15.0, 0.5, 
# MAGIC         0.4, 2.0, 2.0, 1.1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.commitInfo.userMetadata;

# COMMAND ----------

# MAGIC %md
# MAGIC ###2- List the transaction log entries

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter03/YellowTaxisDeltaPartitioned/_delta_log/*.json

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - Look for our custom metadata tag in the commitInfo Action of the Transaction Log Entry

# COMMAND ----------

# MAGIC %sh
# MAGIC grep commit /dbfs/mnt/datalake/book/chapter03/YellowTaxisDeltaPartitioned/_delta_log/00000000000000000006.json > /tmp/commit.json
# MAGIC python -m json.tool /tmp/commit.json

# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - Look for the custom Metadata tag in DESCRIBE HISTORY

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY  taxidb.yellowtaxisPartitioned

# COMMAND ----------

# MAGIC %md
# MAGIC ###5 - Get the schema for the YellowTaxiPartitioned Delta Table

# COMMAND ----------

df = spark.read.format("delta").table("taxidb.YellowTaxisPartitioned")
yellowTaxiSchema = df.schema

# COMMAND ----------

# MAGIC %md
# MAGIC ###6 - Read extra data for the table from an "append" CSV file

# COMMAND ----------

df_for_append = spark.read                            \
                     .option("header", "true")        \
                     .schema(yellowTaxiSchema)        \
                     .csv("/mnt/datalake/book/data files/YellowTaxis_append.csv")

display(df_for_append)

# COMMAND ----------

# MAGIC %md
# MAGIC ###7 - Append the new data, while applying a UserMetaData tag for PII

# COMMAND ----------

df_for_append.write                                              \
            .mode("append")                                      \
            .format("delta")                                     \
            .option("userMetadata", '{"PII": "Confidential XYZ"}') \
            .save("/mnt/datalake/book/chapter03/YellowTaxisDeltaPartitioned")


# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter03/YellowTaxisDeltaPartitioned/_delta_log/*.json

# COMMAND ----------

# MAGIC %md
# MAGIC ###8 - Check for the custom metadata tag in the new Transaction Log Entry

# COMMAND ----------

# MAGIC %sh
# MAGIC grep commit /dbfs/mnt/datalake/book/chapter03/YellowTaxisDeltaPartitioned/_delta_log/00000000000000000005.json > /tmp/commit.json
# MAGIC python -m json.tool /tmp/commit.json
