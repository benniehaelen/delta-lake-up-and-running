# Databricks notebook source
# MAGIC %fs
# MAGIC ls /dluar/data/green_tripdata_2019_12.csv

# COMMAND ----------

INPUT_PATH = '/dluar/data/green_tripdata_2019_12.csv'

df = spark.read.format("csv").option("inferschema", True).option("header", True).load(INPUT_PATH)
df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS greentaxi.tripData
# MAGIC (
# MAGIC     vendorId             INT,
# MAGIC     lpepPickup           TIMESTAMP,
# MAGIC     lpepDropoff          TIMESTAMP,
# MAGIC     storeAndForewardFlag STRING,
# MAGIC     rateCodeID           INT,
# MAGIC     puLocationID         INT,
# MAGIC     doLocationID         INT,
# MAGIC     passengerCount       INT,
# MAGIC     tripDistance         DOUBLE,
# MAGIC     fareAmount           DOUBLE,
# MAGIC     extra                DOUBLE,
# MAGIC     mtaTax               DOUBLE,
# MAGIC     tipAmount            DOUBLE,
# MAGIC     tollsAmount          DOUBLE,
# MAGIC     ehailFee             DOUBLE,
# MAGIC     improvementSurcharge DOUBLE,
# MAGIC     totalAmount          DOUBLE,
# MAGIC     paymentType          INT,
# MAGIC     tripType             INT,
# MAGIC     congestionSurcharge  DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION ('/dluar/greentaxi/tripData/')
