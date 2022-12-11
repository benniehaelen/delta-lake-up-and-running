# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS taxidb.YellowTaxis

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -r "/dbfs/mnt/datalake/book/chapter03/YellowTaxisDelta/"

# COMMAND ----------

# MAGIC %sql
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

# MAGIC %sql
# MAGIC select * from taxidb.YellowTaxis

# COMMAND ----------

df = spark.read.format("delta").table("taxidb.YellowTaxis")
yellowTaxiSchema = df.schema
df.printSchema()

# COMMAND ----------

df_for_append = spark.read                            \
                     .option("header", "true")        \
                     .schema(yellowTaxiSchema)        \
                     .csv("/mnt/datalake/book/data files/YellowTaxis_append.csv")

display(df_for_append)


# COMMAND ----------

df_for_append.write                     \
            .mode("append")             \
            .format("delta")            \
            .save("/mnt/datalake/book/chapter03/YellowTaxisDelta")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     *
# MAGIC FROM 
# MAGIC     taxidb.YellowTaxis

# COMMAND ----------

# MAGIC %fs
# MAGIC cp "dbfs:/FileStore/tables/YellowTaxisLargeAppend.csv" "mnt/datalake/book/DataFiles/YellowTaxisLargeAppend.csv"

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO taxidb.yellowtaxis
# MAGIC FROM (
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
# MAGIC         FROM '/mnt/datalake/book/DataFiles/YellowTaxisLargeAppend.csv' 
# MAGIC )
# MAGIC 
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ("header" = "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*)
# MAGIC FROM    
# MAGIC     taxidb.YellowTaxis

# COMMAND ----------



