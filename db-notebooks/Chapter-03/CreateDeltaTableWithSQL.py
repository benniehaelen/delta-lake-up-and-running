# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS taxidb;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS taxidb.rateCard
# MAGIC (
# MAGIC     rateCodeId   INT,
# MAGIC     rateCodeDesc STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/dluar/tables/rateCard';

# COMMAND ----------

# MAGIC %sql
# MAGIC USE taxidb;
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /dluar/tables/rateCard

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /dluar/tables/rateCard/_delta_log

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE default.rateCard

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS taxidb.rateCard
# MAGIC (
# MAGIC     rateCodeId   INT,
# MAGIC     rateCodeDesc STRING
# MAGIC )

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /user/hive/warehouse/ratecard

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS delta.`/dluar/tables/rateCard_unmanaged`
# MAGIC (
# MAGIC     rateCodeId   INT,
# MAGIC     rateCodeDesc STRING
# MAGIC )

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /dluar/tables/rateCard_unmanaged

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE taxidb;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE taxidb;
# MAGIC DESCRIBE TABLE rateCard;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE taxidb;
# MAGIC DESCRIBE TABLE EXTENDED rateCard;

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Databricks
