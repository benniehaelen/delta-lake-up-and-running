# Databricks notebook source
INPUT_PATH = '/databricks-datasets/nyctaxi/taxizone/taxi_rate_code.csv'
DELTALAKE_PATH = 'dbfs:/mnt/datalake/book/chapter03/createDeltaTableWithDataFrameWriter'

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table taxidb.rateCard;

# COMMAND ----------

# Read the Dataframe from the input path
df_rate_codes = spark                                              \
                .read                                              \
                .format("csv")                                     \
                .option("inferSchema", True)                       \
                .option("header", True)                            \
                .load(INPUT_PATH)

# COMMAND ----------

display(df_rate_codes)

# COMMAND ----------

# Save our DataFrame as a managed Hive table
df_rate_codes.write.format("delta").saveAsTable('taxidb.rateCard')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED taxidb.rateCard;

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /user/hive/warehouse/ratecard

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxidb.ratecard

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop the existing table
# MAGIC drop table if exists taxidb.rateCard;

# COMMAND ----------

# Next, write out the data frame to our 
# Data Lake Path
df_rate_codes                     \
        .write                    \
        .format("delta")          \
        .mode("overwrite")        \
        .save(DELTALAKE_PATH)

# COMMAND ----------

df_rate_codes.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Finally, we create an unmanaged table on top of the Data Lake Path
# MAGIC CREATE OR REPLACE TABLE taxidb.rateCard
# MAGIC (
# MAGIC     RateCodeID   INT,
# MAGIC     RateCodeDesc STRING
# MAGIC )
# MAGIC LOCATION 'dbfs:/mnt/datalake/book/chapter03/createDeltaTableWithDataFrameWriter'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxidb.rateCard

# COMMAND ----------

# MAGIC %fs
# MAGIC head databricks-datasets/nyctaxi/taxizone/taxi_zone_lookup.csv
