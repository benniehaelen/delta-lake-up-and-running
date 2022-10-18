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

# ===============================================
# Step 1 - First write operation
# ===============================================

# Create an array with the columns of our dataframe
columns = ['patientId', 'name', 'city']

# Create the data as an array of tuples
data = [
    (1, 'Patient 1', 'Seattle'),
    (2, 'Patient 2', 'Washington'),
    (3, 'Patient 3', 'Boston'),
    (4, 'Patient 4', 'Houston'),
    (5, 'Patient 5', 'Dallas'),
    (6, 'Patient 6', 'New York')
]

# Create a dataframe from the above array and column
# definitions
df = spark.createDataFrame(data, columns)

# Write out the dataframe as a parquet file.
df.coalesce(1).write.format("delta").save(DATALAKE_PATH)

print("====> Step 1 Completed")
# ===============================================
# Step 2 - Multiple writes in a loop
# ===============================================

# Loop from 0..9
for index in range(9):
    
    # create a patient tuple
    patientId = 10 + index
    t = (patientId, f"Patient {patientId}", "Phoenix")
    
    # Create and write the dataframe
    df = spark.createDataFrame([t], columns)
    df.coalesce(1)             \
      .write                   \
      .format("delta")         \
      .mode("append")          \
      .save(DATALAKE_PATH)

print("====> Step 2 Completed")


# ================================================
# Step 3 - Another append - trigger the checkpoint
#          file creation
# ================================================

patientId = 100
t = (patientId, f"Patient {patientId}", "Phoenix")

df = spark.createDataFrame([t], columns)
df.coalesce(1).write.format("delta").mode("append").save(DATALAKE_PATH)

print("====> Step 3 Completed")

# ================================================
# Step 4 - Two more appends to the file
# ================================================
    
for index in range(2):
    patientId = 200 + index
    t = (patientId, f"Patient {patientId}", "Phoenix")
    df = spark.createDataFrame([t], columns)
    df.coalesce(1)              \
      .write                    \
      .format("delta")          \
      .mode("append")           \
      .save(DATALAKE_PATH)    

print("====> Step 4 Completed")
