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
# MAGIC                 This notebook performs a MERGE operation and shows the impact on the part files and the details of what
# MAGIC                 is recorded in the transaction log.
# MAGIC 
# MAGIC                 
# MAGIC      The following actions are taken in this notebook:
# MAGIC        1 - ...
# MAGIC 
# MAGIC    

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*)
# MAGIC FROM 
# MAGIC     taxidb.YellowTaxis

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     *
# MAGIC FROM    
# MAGIC     taxidb.YellowTaxis
# MAGIC WHERE   
# MAGIC     RideId > 9999990

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  
# MAGIC     *
# MAGIC FROM
# MAGIC     taxidb.yellowtaxis
# MAGIC WHERE 
# MAGIC     rideId = 100000

# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - Get the YellowTaxi schema from our table

# COMMAND ----------

df = spark.read.format("delta").table("taxidb.YellowTaxis")
yellowTaxiSchema = df.schema
print(yellowTaxiSchema)

# COMMAND ----------

# MAGIC %md
# MAGIC ###5 - Read the source Merge Data File

# COMMAND ----------

yellowTaxisMergeDataFrame = spark      \
            .read                      \
            .option("header", "true")  \
            .schema(yellowTaxiSchema)  \
            .csv("/mnt/datalake/book/chapter04/YellowTaxisMergeData.csv") \
            .sort(col("RideId"))

display(yellowTaxisMergeDataFrame)                            


# COMMAND ----------

# MAGIC %md
# MAGIC ###6 - Create a Temporary View on top of our DataFrame, that way we can use it in SQL

# COMMAND ----------

# Create a Temporary View on top of our DataFrame, making it 
# accessible to the SQL MERGE statement below
yellowTaxisMergeDataFrame.createOrReplaceTempView("YellowTaxiMergeData")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     *
# MAGIC FROM    
# MAGIC     YellowTaxiMergeData

# COMMAND ----------

# MAGIC %md
# MAGIC ###7 - Execute our MERGE statement

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO taxidb.YellowTaxis target
# MAGIC     USING YellowTaxiMergeData source
# MAGIC         ON target.RideId = source.RideId
# MAGIC 
# MAGIC -- We need to update the VendorId if the records
# MAGIC -- matched
# MAGIC WHEN MATCHED                                       
# MAGIC     THEN
# MAGIC         -- If you want to update all columns, 
# MAGIC         -- you can say "SET *"
# MAGIC         UPDATE SET target.VendorId = source.VendorId
# MAGIC WHEN NOT MATCHED
# MAGIC     THEN
# MAGIC         -- If all columns match, you can also do a "INSERT *"
# MAGIC         INSERT(RideId, VendorId, PickupTime, DropTime, PickupLocationId, 
# MAGIC                DropLocationId, CabNumber, DriverLicenseNumber, PassengerCount,
# MAGIC                TripDistance, RateCodeId, PaymentType, TotalAmount, FareAmount,
# MAGIC                Extra, MtaTax, TipAmount, TollsAmount, ImprovementSurCharge)
# MAGIC         VALUES(RideId, VendorId, PickupTime, DropTime, PickupLocationId, 
# MAGIC                DropLocationId, CabNumber, DriverLicenseNumber, PassengerCount,
# MAGIC                TripDistance, RateCodeId, PaymentType, TotalAmount, FareAmount,
# MAGIC                Extra, MtaTax, TipAmount, TollsAmount, ImprovementSurCharge)

# COMMAND ----------

# MAGIC %md
# MAGIC ###8 - Perform some validation on the table

# COMMAND ----------

# MAGIC %sql
# MAGIC --Make sure that we have a record with VendorId = 100,000
# MAGIC SELECT
# MAGIC     *
# MAGIC FROM    
# MAGIC     taxidb.yellowtaxis
# MAGIC WHERE 
# MAGIC     RideId = 100000

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Make sure that the VendorId has been updated
# MAGIC -- for the records with RideId between
# MAGIC -- 9,999,991 and 9,999,995
# MAGIC SELECT 
# MAGIC     RideId, 
# MAGIC     VendorId
# MAGIC FROM 
# MAGIC     taxidb.yellowtaxis
# MAGIC WHERE RideId BETWEEN 9999991 and 9999995

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  
# MAGIC     *
# MAGIC FROM 
# MAGIC     taxidb.yellowtaxis
# MAGIC WHERE 
# MAGIC     RideId > 9999995

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  
# MAGIC     COUNT(*)
# MAGIC FROM    
# MAGIC     taxidb.YellowTaxis

# COMMAND ----------

# MAGIC %md
# MAGIC ###9 - Use DESCRIBE HISTORY to see the impact of our MERGE statement

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY taxidb.YellowTaxis

# COMMAND ----------

# MAGIC %md
# MAGIC ###10 - Perform a directory listing of our Delta File

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter04/YellowTaxisDelta/*.parquet
