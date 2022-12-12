# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists taxidb.YellowTaxisPartitioned;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE taxidb.YellowTaxisPartitioned
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
# MAGIC PARTITIONED BY(VendorId)
# MAGIC LOCATION "/mnt/datalake/book/chapter03/YellowTaxisDeltaPartitioned"

# COMMAND ----------

input_df = spark.read.format("delta").table("taxidb.YellowTaxis")

# COMMAND ----------

input_df                                                               \
    .write                                                             \
    .format("delta")                                                   \
    .mode("overwrite")                                                 \
    .save("/mnt/datalake/book/chapter03/YellowTaxisDeltaPartitioned")


# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter03/YellowTaxisDeltaPartitioned

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC     VendorId,
# MAGIC     count(*)
# MAGIC FROM 
# MAGIC     taxidb.yellowtaxis
# MAGIC GROUP BY 
# MAGIC     VendorId
# MAGIC ORDER BY 
# MAGIC     COUNT(*) DESC

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter03/YellowTaxisDeltaPartitioned/VendorId=4

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT 
# MAGIC     DISTINCT(VendorId) 
# MAGIC FROM
# MAGIC     taxidb.YellowTaxisPartitioned;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COUNT(*) > 0 AS `Partition exists`
# MAGIC FROM 
# MAGIC     taxidb.YellowTaxisPartitioned
# MAGIC WHERE 
# MAGIC     VendorId = 2

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE taxidb.YellowTaxisPartitioned
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
# MAGIC PARTITIONED BY(VendorId, RateCodeId)
# MAGIC LOCATION "/mnt/datalake/book/chapter03/YellowTaxisDeltaPartitioned"
