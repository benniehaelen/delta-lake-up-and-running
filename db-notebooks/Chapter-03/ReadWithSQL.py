# Databricks notebook source
# MAGIC %sql
# MAGIC use greentaxi;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.ratecard;

# COMMAND ----------

df = spark.sql("select * from default.ratecard")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended default.ratecard;

# COMMAND ----------

df = spark.read.format("delta").load("dbfs:/dluar/ch03/createDeltaTableWithDataFrameWriter")
display(df)

# COMMAND ----------

df = spark.read.format("delta").table("default.ratecard")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.ratecard;

# COMMAND ----------

display(_sqldf)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets/nyctaxi/tables/nyctaxi_yellow
