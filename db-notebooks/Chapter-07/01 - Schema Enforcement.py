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
# MAGIC                 This notebook contains the code for the schema enforcement section of the chapter
# MAGIC
# MAGIC                 
# MAGIC      The following actions are taken in this notebook:
# MAGIC        1 - Show the schemaString of the metaData section of the transaction log entry
# MAGIC        2 - Append a DataFrame with a matching schema to the table, which of source 
# MAGIC            succeeds without any problems.
# MAGIC        3 - Next, we add an additional column to the DataFrame, and try to append that
# MAGIC            DataFrame to the table. This will result in a "schema mismatch" operation, 
# MAGIC            and no data should be written at all.
# MAGIC    

# COMMAND ----------

# Make sure to import the StructType and all supporting
# cast of Type classes (StringType, IntegerType etc..)
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1 - Display the schemaString 

# COMMAND ----------

# MAGIC %sh
# MAGIC # The schemaString is part of the metaData action of the Transaction Log entry
# MAGIC # The schemaString contains the full schema of the Delta file at the time that the 
# MAGIC # log entry was written
# MAGIC grep "metadata" /dbfs/mnt/datalake/book/chapter07/TaxiRateCode/_delta_log/00000000000000000000.json > /tmp/commit.json
# MAGIC python -m json.tool < /tmp/commit.json

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2 - Append a DataFrame with a matching schema

# COMMAND ----------

# DBTITLE 1,Append a schema-compliant DataFrame to the table
# Define the schema for the DataFrame
# Notice that the columns match the table schema
schema = StructType([
    StructField("RateCodeId", IntegerType(), True),
    StructField("RateCodeDesc", StringType(), True)
])

# Create a list of rows for the DataFrame
data = [(10, "Rate Code 10"), (11, "Rate Code 11"), (12, "Rate Code 12")]

# Create a DataFrame, passing in the data rows
# and the schema
df = spark.createDataFrame(data, schema)

# Perform the write. This write will succeed without any
# problems
df.write           \
  .format("delta") \
  .mode("append")  \
  .save("/mnt/datalake/book/chapter07/TaxiRateCode")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validate that all data has been written
# MAGIC select * from taxidb.TaxiRateCode order by RateCodeId

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3 - Add an additional column to the DataFrame 

# COMMAND ----------

# DBTITLE 1,Attempt to write a DataFrame with an additional Column
# Define the schema for the DataFrame
# Notice that we added an additional column
schema = StructType([
    StructField("RateCodeId", IntegerType(), True),
    StructField("RateCodeDesc", StringType(), True),
    StructField("RateCodeName", StringType(), True)
])

# Create a list of rows for the DataFrame
data = [
    (15, "Rate Code 15", "C15"),
    (16, "Rate Code 16", "C16"),
    (17, "Rate Code 17", "C17")]

# Create a DataFrame from the list of rows and the schema
df = spark.createDataFrame(data, schema)

# Attempt to append the DataFrame to the table
df.write           \
  .format("delta") \
  .mode("append")  \
  .save("/mnt/datalake/book/chapter07/TaxiRateCode")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validate that the previous operation did not write any data
# MAGIC -- Verify that no RateCodeId >= 15
# MAGIC -- records are present in the table
# MAGIC select * from delta.`/mnt/datalake/book/chapter07/TaxiRateCode`

# COMMAND ----------

# MAGIC %sh
# MAGIC # Create a listing of all transaction log entries. 
# MAGIC # We notice that there are only two entries.
# MAGIC # The first entry represents the creation of the table
# MAGIC # The second entry is the append of the valid dataframe
# MAGIC # There is no entry for the above code since the exception 
# MAGIC # occured, resulting in a rollback of the transaction
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter07/TaxiRateCode/_delta_log/*.json

# COMMAND ----------

# MAGIC %sh
# MAGIC # Validate the the 1.json entry is indeed the append of the valid data
# MAGIC cat /dbfs/mnt/datalake/book/chapter07/TaxiRateCode/_delta_log/00000000000000000001.json 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Look at the history for the Delta table
# MAGIC DESCRIBE HISTORY delta.`/mnt/datalake/book/chapter07/TaxiRateCode`
