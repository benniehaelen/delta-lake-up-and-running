# Databricks notebook source
INPUT_PATH    = '/databricks-datasets/nyctaxi/taxizone/taxi_rate_code.csv'
DATALAKE_PATH = '/dluar/ch03/SaveDeltaFile'

# COMMAND ----------

dbutils.fs.rm(DATALAKE_PATH, recurse=True)

# COMMAND ----------


df_rate_codes = spark                                              \
                .read                                              \
                .format("csv")                                     \
                .option("inferSchema", True)                       \
                .option("header", True)                            \
                .load(INPUT_PATH)

# COMMAND ----------

df_rate_codes.printSchema()

# COMMAND ----------

display(df_rate_codes)

# COMMAND ----------

df_rate_codes.write.format("delta").save(DATALAKE_PATH)

# COMMAND ----------

display(dbutils.fs.ls(DATALAKE_PATH))

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets/nyctaxi/tripdata/green

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets/nyctaxi/reference
