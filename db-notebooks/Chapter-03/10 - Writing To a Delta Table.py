# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />   
# MAGIC 
# MAGIC  Name:          chapter 03/10 - Writing to a Delta Table
# MAGIC  
# MAGIC      Author:    Bennie Haelen
# MAGIC      Date:      12-10-2022
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 3 of the book - Basic Operations on Delta Tables.
# MAGIC                 This notebook illustrates how to write to a Delta Table with both SQL and PySpark
# MAGIC 
# MAGIC                 
# MAGIC      The following Delta Lake functionality is demonstrated in this notebook:
# MAGIC        1 - Drop the YellowTaxis Delta Table and delete its Delta Files.
# MAGIC        2 - Re-build the Yellow Taxis Delta Table with a SQL statement
# MAGIC        3 - Insert data with a SQL statement
# MAGIC        4 - Perform a SQL Select on the Delta Table to ensure that the row inserted was indeed loaded
# MAGIC        5 - Load the schema from the Delta Table
# MAGIC        6 - Load a DataFrame from a CSV file, specifying the table schema
# MAGIC        7 - Append this DataFrame to our YellowTaxis Delta Table
# MAGIC        8 - Use Copy Into to append the contents of a large .CSV file into our Delta Table

# COMMAND ----------

# MAGIC %md
# MAGIC ###1 - Drop the YellowTaxis table and it's underlying Delta Files

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop the YellowTaxis table. Since this is an unmamaged
# MAGIC -- table, this will NOT remove the underlying files
# MAGIC DROP TABLE IF EXISTS taxidb.YellowTaxis

# COMMAND ----------

# MAGIC %sh
# MAGIC # Remove the Deta Files, since they are not automatically 
# MAGIC # dropped for a managed table
# MAGIC rm -r "/dbfs/mnt/datalake/book/chapter03/YellowTaxisDelta/"

# COMMAND ----------

# MAGIC %md
# MAGIC ###2 - Rebuild the YellowTaxis table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Re-create YellowTaxis as an unmanaged table
# MAGIC CREATE TABLE taxidb.YellowTaxis
# MAGIC (
# MAGIC     RideId                  INT,
# MAGIC     VendorId                INT,
# MAGIC     PickupTime              TIMESTAMP,
# MAGIC     DropTime                TIMESTAMP,
# MAGIC     PickupLocationId        INT,
# MAGIC     DropLocationId          INT,
# MAGIC     CabNumber               STRING,
# MAGIC     DriverLicenseNumber     STRING,
# MAGIC     PassengerCount          INT,
# MAGIC     TripDistance            DOUBLE,
# MAGIC     RatecodeId              INT,
# MAGIC     PaymentType             INT,
# MAGIC     TotalAmount             DOUBLE,
# MAGIC     FareAmount              DOUBLE,
# MAGIC     Extra                   DOUBLE,
# MAGIC     MtaTax                  DOUBLE,
# MAGIC     TipAmount               DOUBLE,
# MAGIC     TollsAmount             DOUBLE,         
# MAGIC     ImprovementSurcharge    DOUBLE
# MAGIC     
# MAGIC ) USING DELTA         
# MAGIC LOCATION "/mnt/datalake/book/chapter03/YellowTaxisDelta"

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - Insert data with a SQL statement

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO taxidb.yellowtaxis
# MAGIC (RideId, VendorId, PickupTime, DropTime, 
# MAGIC  PickupLocationId, DropLocationId, CabNumber, 
# MAGIC  DriverLicenseNumber, PassengerCount, TripDistance, 
# MAGIC  RatecodeId, PaymentType, TotalAmount,
# MAGIC  FareAmount, Extra, MtaTax, TipAmount, 
# MAGIC  TollsAmount, ImprovementSurcharge)
# MAGIC 
# MAGIC  VALUES(9999995, 1, '2019-11-01T00:00:00.000Z', 
# MAGIC         '2019-11-01T00:02:23.573Z', 65, 71, 'TAC304',
# MAGIC         '453987', 2, 4.5, 1, 1, 20.34, 15.0, 0.5, 
# MAGIC         0.4, 2.0, 2.0, 1.1)

# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - Performa SELECT * from the Delta Table to ensure that the data was indeed loaded

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxidb.YellowTaxis

# COMMAND ----------

# MAGIC %md
# MAGIC ###5 - Load the schema from the Delta Table

# COMMAND ----------

df = spark.read.format("delta").table("taxidb.YellowTaxis")
yellowTaxiSchema = df.schema
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###6 - Load a DataFrame from a CSV file, specifying the table schema

# COMMAND ----------

# Note that we use the schema retrieved previously
# Since our CSV file has a header, we need to specify that with the .option() method
df_for_append = spark.read                            \
                     .option("header", "true")        \
                     .schema(yellowTaxiSchema)        \
                     .csv("/mnt/datalake/book/data files/YellowTaxis_append.csv")

display(df_for_append)


# COMMAND ----------

# MAGIC %md
# MAGIC ##7 - Append the DataFrame from the .csv file to the YellowTaxis Delta Table

# COMMAND ----------

# We are using the mode "append" here to append to the Delta Table
df_for_append.write                     \
            .mode("append")             \
            .format("delta")            \
            .save("/mnt/datalake/book/chapter03/YellowTaxisDelta")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We should now have five rows
# MAGIC SELECT
# MAGIC     *
# MAGIC FROM 
# MAGIC     taxidb.YellowTaxis

# COMMAND ----------

# MAGIC %md
# MAGIC ###8 - Use Copy Into to append the contents of a large .CSV file into our Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO taxidb.yellowtaxis
# MAGIC FROM (
# MAGIC     -- Not that we specify the data types here
# MAGIC     -- since a .CSV file would only contain
# MAGIC     -- strings
# MAGIC     SELECT     RideId::Int
# MAGIC              , VendorId::Int
# MAGIC              , PickupTime::Timestamp
# MAGIC              , DropTime::Timestamp
# MAGIC              , PickupLocationId::Int
# MAGIC              , DropLocationId::Int
# MAGIC              , CabNumber::String
# MAGIC              , DriverLicenseNumber::String
# MAGIC              , PassengerCount::Int
# MAGIC              , TripDistance::Double
# MAGIC              , RateCodeId::Int
# MAGIC              , PaymentType::Int
# MAGIC              , TotalAmount::Double
# MAGIC              , FareAmount::Double
# MAGIC              , Extra::Double
# MAGIC              , MtaTax::Double
# MAGIC              , TipAmount::Double
# MAGIC              , TollsAmount::Double
# MAGIC              , ImprovementSurcharge::Double
# MAGIC              
# MAGIC         FROM '/mnt/datalake/book/chapter03/YellowTaxisLargeAppend.csv' 
# MAGIC )
# MAGIC 
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ("header" = "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ###9 - Run the COPY INTO again, since it keeps track of what files are loaded it will not load the records again

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO taxidb.yellowtaxis
# MAGIC FROM (
# MAGIC     -- Not that we specify the data types here
# MAGIC     -- since a .CSV file would only contain
# MAGIC     -- strings
# MAGIC     SELECT     RideId::Int
# MAGIC              , VendorId::Int
# MAGIC              , PickupTime::Timestamp
# MAGIC              , DropTime::Timestamp
# MAGIC              , PickupLocationId::Int
# MAGIC              , DropLocationId::Int
# MAGIC              , CabNumber::String
# MAGIC              , DriverLicenseNumber::String
# MAGIC              , PassengerCount::Int
# MAGIC              , TripDistance::Double
# MAGIC              , RateCodeId::Int
# MAGIC              , PaymentType::Int
# MAGIC              , TotalAmount::Double
# MAGIC              , FareAmount::Double
# MAGIC              , Extra::Double
# MAGIC              , MtaTax::Double
# MAGIC              , TipAmount::Double
# MAGIC              , TollsAmount::Double
# MAGIC              , ImprovementSurcharge::Double
# MAGIC              
# MAGIC         FROM '/mnt/datalake/book/chapter03/YellowTaxisLargeAppend.csv' 
# MAGIC )
# MAGIC 
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ("header" = "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ###10 - Check the final row count

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*)
# MAGIC FROM    
# MAGIC     taxidb.YellowTaxis

# COMMAND ----------

# MAGIC 
# MAGIC %fs
# MAGIC ls /mnt/datalake/book/DataFiles
