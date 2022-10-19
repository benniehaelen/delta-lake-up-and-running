# Databricks notebook source
INPUT_PATH    = '/databricks-datasets/nyctaxi/taxizone/taxi_rate_code.csv'

# COMMAND ----------


df_rate_codes = spark                                              \
                .read                                              \
                .format("csv")                                     \
                .option("inferSchema", True)                       \
                .option("header", True)                            \
                .load(INPUT_PATH)

# COMMAND ----------

display(df_rate_codes)

# COMMAND ----------

df_rate_codes.write.format("delta").saveAsTable('RateCard')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets/nyctaxi/tables/nyctaxi_yellow

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases
