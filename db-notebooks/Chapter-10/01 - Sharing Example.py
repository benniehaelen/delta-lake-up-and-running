# Databricks notebook source
import delta_sharing
import pandas as pd

# COMMAND ----------

# Point to the profile file. IT can be a file on the local
# file system or remote file system. In this case, we have
# uploaded the file to dbfs
profile_path = "/dbfs/mnt/datalake/book/delta-sharing/open-datasets.share"

# COMMAND ----------

# Create a SharingClient and list all shared tables
client = delta_sharing.SharingClient(profile_path)
client.list_all_tables()

# COMMAND ----------

# Create a URL to access a shared table
# A table path is the proile path following with 
# ('<share-name>.<schema_name>.<table_name>)
# Here, we are loading the table as a pandas DataFrame
table_url = profile_path + "#delta_sharing.default.boston-housing"
df = delta_sharing.load_as_pandas(table_url, limit=5)
display(df)

# COMMAND ----------

# We can also access the shared table with Spark. Note that we have to use the
# dbfs:/ path prefix here 
profile_path_spark = "dbfs:/mnt/datalake/book/delta-sharing/open-datasets.share"
table_url_spark = profile_path_spark + "#delta_sharing.default.boston-housing"

df_spark = delta_sharing.load_as_spark(table_url_spark)
display(df_spark.limit(5))

# COMMAND ----------

table_url_spark = profile_path_spark + "#delta_sharing.default.owid-covid-data"
print(table_url_spark)

df_cdf = delta_sharing.load_table_changes_as_spark(table_url_spark, starting_version=0, ending_version=1)
display(df_cdf)
