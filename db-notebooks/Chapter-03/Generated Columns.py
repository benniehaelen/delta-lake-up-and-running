# Databricks notebook source
from delta.tables import *

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE taxidb.YellowTaxis
# MAGIC (
# MAGIC     RideId               INT        COMMENT 'This is our primary Key column',
# MAGIC     VendorId             INT,
# MAGIC     PickupTime           TIMESTAMP,
# MAGIC     PickupYear           INT        GENERATED ALWAYS AS(YEAR  (PickupTime)),
# MAGIC     PickupMonth          INT        GENERATED ALWAYS AS(MONTH (PickupTime)),
# MAGIC     PickupDay            INT        GENERATED ALWAYS AS(DAY   (PickupTime)),
# MAGIC     DropTime             TIMESTAMP,
# MAGIC     CabNumber            STRING     COMMENT 'Official Yellow Cab Number'
# MAGIC ) USING DELTA
# MAGIC LOCATION "/mnt/datalake/book/chapter03/YellowTaxis.delta"
# MAGIC COMMENT 'Table to store Yellow Taxi data'

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM taxidb.YellowTaxis

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO taxidb.YellowTaxis
# MAGIC     (RideId, VendorId, PickupTime, DropTime, CabNumber)
# MAGIC VALUES
# MAGIC     (5, 101, '2021-7-1T8:43:28UTC+3', '2021-7-1T8:43:28UTC+3', '51-986')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT PickupTime, PickupYear, PickupMonth, PickupDay FROM taxidb.YellowTaxis

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE default.dummy
# MAGIC (
# MAGIC     ID   STRING GENERATED ALWAYS AS (UUID()),
# MAGIC     Name STRING
# MAGIC ) USING DELTA
