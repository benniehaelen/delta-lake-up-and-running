# Databricks notebook source
# MAGIC %sql
# MAGIC DESCRIBE DATABASE taxidb;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE taxidb.rateCard;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED taxidb.rateCard;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED taxidb.ratecardmanaged
