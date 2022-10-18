from tabnanny import check
import pyspark
from delta import *

# Create a builder with the Delta extension
builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Create a Spark instance with the builder
# As a result, we now can read and write Delta files
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Set our output path for our Delta files
DATALAKE_PATH = "/book/chapter02/transactionLogCheckPointExample"
CHECKPOINT_PATH = "/_delta_log/00000000000000000010.checkpoint.parquet"
# Read the checkpoint.parquet file
checkpoint_df =                                \
  spark                                        \
  .read                                      \
  .format("parquet")                         \
  .load(f"{DATALAKE_PATH}{CHECKPOINT_PATH}")

# Display the checkpoint dataframe
checkpoint_df.show()
