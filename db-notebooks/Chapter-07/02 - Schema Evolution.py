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
# MAGIC                 This notebook handles the use cases for schema evolution
# MAGIC                 
# MAGIC
# MAGIC                 
# MAGIC      The following actions are taken in this notebook:
# MAGIC        1 - Verify that the autoMerge setting for the cluster is set to false
# MAGIC        2 - ...
# MAGIC    

# COMMAND ----------

# Make sure to import the StructType and all supporting
# cast of Type classes (StringType, IntegerType etc..)
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Perform some cleanup from the previous chapter
# MAGIC %sql
# MAGIC delete from taxidb.taxiratecode where RateCodeId >= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxidb.taxiratecode

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1 - Verify that autoMerge at the cluster level is set to false

# COMMAND ----------

# Get the cluster-wide autoMerge setting
is_automerge_enabled = \
    spark.conf.get('spark.databricks.delta.schema.autoMerge.enabled')
print(f"Auto Merge  is set to: {is_automerge_enabled}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2 - Adding a column (Use Case # 1)

# COMMAND ----------

# Define the schema for the DataFrame
# Notice the additional RateCodeName column, which
# is not part of the target table schema
schema = StructType([
    StructField("RateCodeId", IntegerType(), True),
    StructField("RateCodeDesc", StringType(), True),
    StructField("RateCodeName", StringType(), True)
])

# Create a list of rows for the DataFrame
data = [
    (20, "Rate Code 20", "C20"), 
    (21, "Rate Code 21", "C21"), 
    (22, "Rate Code 22", "C22")
]

# Create a DataFrame from the list of rows and the schema
df = spark.createDataFrame(data, schema)

# Append the DataFrame to the Delta Table
df.write                         \
  .format("delta")               \
  .option("mergeSchema", "true") \
  .mode("append")                \
  .save("/mnt/datalake/book/chapter07/TaxiRateCode.delta")

# Print the schema
df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT 
# MAGIC       * 
# MAGIC   FROM
# MAGIC       delta.`/mnt/datalake/book/chapter07/TaxiRateCode.delta`
# MAGIC ORDER BY
# MAGIC     RateCodeId

# COMMAND ----------

# MAGIC %sh
# MAGIC grep "metadata" /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/00000000000000000003.json > /tmp/commit.json
# MAGIC python -m json.tool < /tmp/commit.json

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3 - Missing Data Column in Source Data Frame (Use Case #2)

# COMMAND ----------

# Define the schema for the DataFrame
schema = StructType([
    StructField("RateCodeId", IntegerType(), True),
    StructField("RateCodeName", StringType(), True)
])

# Create a list of rows for the DataFrame
data = [(30, "C30"), (31, "C31"), (32, "C32")]

# Create a DataFrame from the list of rows and the schema
df = spark.createDataFrame(data, schema)

# Append the DataFrame to the table
df.write                         \
  .format("delta")               \
  .option("mergeSchema", "true") \
  .mode("append")                \
  .save("/mnt/datalake/book/chapter07/TaxiRateCode.delta")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- The previously existing rows are not changed
# MAGIC -- The RateCodeDesc for the new rows are null, since 
# MAGIC -- they were not present in the source dataframe
# MAGIC SELECT 
# MAGIC     *
# MAGIC FROM 
# MAGIC     delta.`/mnt/datalake/book/chapter07/TaxiRateCode.delta`
# MAGIC ORDER BY
# MAGIC     RateCodeId

# COMMAND ----------

# MAGIC %sh
# MAGIC # We see the part files being added
# MAGIC cat /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/00000000000000000004.json
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4 - Changing a Column data type (Use Case 3)

# COMMAND ----------

# We first start out by cleaning up our table, so we can start fresh
dbutils.fs.rm("dbfs:/mnt/datalake/book/chapter07/TaxiRateCode.delta", recurse=True)

# COMMAND ----------

# MAGIC %sh
# MAGIC # Verify our cleanup
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop the table
# MAGIC drop table if exists taxidb.taxiratecode;

# COMMAND ----------

# Read our CSV data, and change the data type of 
# the RateCodeId to short
df = spark.read.format("csv")      \
        .option("header", "true") \
        .load("/mnt/datalake/book/chapter07/TaxiRateCode.csv")
df = df.withColumn("RateCodeId", df["RateCodeId"].cast(ShortType()))

# Write in Delta Lake format
df.write.format("delta")   \
        .mode("overwrite") \
        .save("/mnt/datalake/book/chapter07/TaxiRateCode.delta")
    
# print the schema
df.printSchema()

# COMMAND ----------

# MAGIC %sh
# MAGIC # Check our tranaction log entries
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/*.json

# COMMAND ----------

# MAGIC %sh
# MAGIC # In our transaction log we will see the commitInfo action and
# MAGIC # the metadata action with our schema, confirming the short data type
# MAGIC # We also have an "add" action with the first part file
# MAGIC cat /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/00000000000000000000.json
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Change the RateCodeId back to an Integer type
# Define the schema for the DataFrame
# Note that we now define the RateCodeId to be an
# Integer type
schema = StructType([
    StructField("RateCodeId", IntegerType(), True),
    StructField("RateCodeDesc", StringType(), True)
])

# Create a list of rows for the DataFrame
data = [(20, "Rate Code 20"), (21, "Rate Code 21"), (22, "Rate Code 22")]

# Create a DataFrame from the list of rows and the schema
df = spark.createDataFrame(data, schema)

# Write the DataFrame with Schema Evolution
df.write                         \
  .format("delta")               \
  .option("mergeSchema", "true") \
  .mode("append")                \
  .save("/mnt/datalake/book/chapter07/TaxiRateCode.delta")

# Print the schema
df.printSchema()

# COMMAND ----------

# MAGIC %sh
# MAGIC # We now see our additional Transaction Entry
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/*.json

# COMMAND ----------

# MAGIC %sh
# MAGIC # When we look at the metadata, we can see that RateCodeId indeed has the integer data type now
# MAGIC grep "metadata" /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/00000000000000000001.json > /tmp/commit.json
# MAGIC python -m json.tool < /tmp/commit.json

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The SELECT below shows that our data is still intact
# MAGIC select * from delta.`/mnt/datalake/book/chapter07/TaxiRateCode.delta`

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 5 - Handling NullTypes (Use Case 4)

# COMMAND ----------

# Define the schema for the DataFrame
# Notice the NullType for RateCodeExp
schema = StructType([
    StructField("RateCodeId", IntegerType(), True),
    StructField("RateCodeDesc", StringType(), True),
    StructField("RateCodeExp", NullType(), True)
])

# Create a list of rows for the DataFrame, note
# that we can only specify None for the NullType
# column
data = [
    (50, "Rate Code 50", None), 
    (51, "Rate Code 51", None), 
    (52, "Rate Code 52", None)]

# Create a DataFrame from the list of rows and the schema
df = spark.createDataFrame(data, schema)

# Write the DataFrame with Schema Evolution
df.write                         \
  .format("delta")               \
  .option("mergeSchema", "true") \
  .mode("append")                \
  .save("/mnt/datalake/book/chapter07/TaxiRateCode.delta")

# Print the schema
df.printSchema()

# COMMAND ----------

# MAGIC %sh
# MAGIC # Notice our latest transaction log entry
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/*.json

# COMMAND ----------

# MAGIC %sh
# MAGIC # We can see that the RateCodeExp column has a void (aka NullType) data type
# MAGIC grep "metadata" /dbfs/mnt/datalake/book/chapter07/TaxiRateCode.delta/_delta_log/00000000000000000002.json > /tmp/commit.json
# MAGIC python -m json.tool < /tmp/commit.json

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   delta.`/mnt/datalake/book/chapter07/TaxiRateCode.delta`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   RateCodeId,
# MAGIC   RateCodeDesc
# MAGIC FROM
# MAGIC   delta.`/mnt/datalake/book/chapter07/TaxiRateCode.delta`
