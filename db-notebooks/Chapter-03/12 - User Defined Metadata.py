# Databricks notebook source
from delta.tables import *

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

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter03/YellowTaxisDeltaPartitioned/_delta_log/*.json

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY  taxidb.yellowtaxisPartitioned

# COMMAND ----------

# MAGIC %sh
# MAGIC grep commit /dbfs/mnt/datalake/book/chapter03/YellowTaxisDeltaPartitioned/_delta_log/00000000000000000007.json > /tmp/commit.json
# MAGIC python -m json.tool /tmp/commit.json

# COMMAND ----------

df = spark.read.format("delta").table("taxidb.YellowTaxisPartitioned")
yellowTaxiSchema = df.schema

# COMMAND ----------

df_for_append = spark.read                            \
                     .option("header", "true")        \
                     .schema(yellowTaxiSchema)        \
                     .csv("/mnt/datalake/book/data files/YellowTaxis_append.csv")

display(df_for_append)

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

# MAGIC %sh
# MAGIC grep commit /dbfs/mnt/datalake/book/chapter03/YellowTaxisDeltaPartitioned/_delta_log/00000000000000000011.json > /tmp/commit.json
# MAGIC python -m json.tool /tmp/commit.json
