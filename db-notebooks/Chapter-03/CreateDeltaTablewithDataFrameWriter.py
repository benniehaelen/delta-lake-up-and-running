# Databricks notebook source
INPUT_PATH = '/databricks-datasets/nyctaxi/taxizone/taxi_rate_code.csv'
DELTALAKE_PATH = '/dluar/ch03/createDeltaTableWithDataFrameWriter'

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

# Save our DataFrame as a managed Hive table
df_rate_codes.write.format("delta").saveAsTable('default.rateCard')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED default.rateCard;

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /user/hive/warehouse/ratecard

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists default.rateCard;

# COMMAND ----------

# First, write out the data frame to our 
# Data Lake Path
df_rate_codes                     \
        .write                    \
        .format("delta")          \
        .mode("overwrite")        \
        .save('/dluar/chapter03/createDeltaTableWithDataFrameWriter')

# COMMAND ----------

df_rate_codes.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Next, we create an unmanaged table on top of the Data Lake Path
# MAGIC CREATE OR REPLACE TABLE default.rateCard
# MAGIC (
# MAGIC     RateCodeID   INT,
# MAGIC     RateCodeDesc STRING
# MAGIC )
# MAGIC LOCATION '/dluar/ch03/createDeltaTableWithDataFrameWriter'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.rateCard
