from msilib.schema import Condition
import pyspark
from pyspark.sql.functions import col, lit
from delta import *
from delta.tables import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Create an array with the columns of our dataframe
columns = ['patientId', 'name']

DATALAKE_PATH = "/book/chapter02/UpdateOperation"
# =======================================================
# Step 1 - First write
# =======================================================

# Create the data as an array of tuples
data = [
    (1, 'P1'),
    (2, 'P2'),
    (3, 'P3'),
    (4, 'P4')
]

# Create a dataframe from the above array and column
# definitions
df = spark.createDataFrame(data, columns)

# Write out the dataframe as a parquet file.
df.coalesce(2)        \
  .write              \
  .format("delta")    \
  .mode("overwrite")  \
  .save(DATALAKE_PATH)

# =======================================================
# Step 2 - Append more patients
# =======================================================

# Create the data as an array of tuples
data = [
    (5, 'P5'),
    (6, 'P6')
]

# Create a dataframe from the above array and column
# definitions
df = spark.createDataFrame(data, columns)

# Write out the dataframe as a parquet file.
df.coalesce(1).write.format("delta").mode("append").save(DATALAKE_PATH)

# =======================================================
# Step 3 - Update Operation
# =======================================================
deltaTable = DeltaTable \
 .forPath(spark, DATALAKE_PATH)
            
deltaTable.update(
  condition = col("patientId") == 1,
  set = { 'name': lit("p11")}
)

# =======================================================
# Step 4 - Re-read to make sure the info is correct
# =======================================================
df = spark.read.format("delta").load(DATALAKE_PATH)
df.show()
