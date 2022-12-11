# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create a managed table in Delta format
# MAGIC CREATE TABLE IF NOT EXISTS taxidb.rateCardManaged
# MAGIC (
# MAGIC     rateCodeId          INT,
# MAGIC     rateCodeDescription STRING
# MAGIC ) USING DELTA

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/user/hive/warehouse/taxidb.db/ratecardmanaged

# COMMAND ----------

# MAGIC %sql
# MAGIC use taxidb;
# MAGIC show tables;
