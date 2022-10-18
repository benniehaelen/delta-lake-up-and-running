import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

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
df.coalesce(1).write                                          \
              .format("delta")                                \
              .save('/book/chapter02/multipleWriteOperations') 

# Create the data as an array of tuples
data = [
    (7, 'Patient 7', 'Phoenix')
]

# Create a dataframe from the above array and column
# definitions
df = spark.createDataFrame(data, columns)

# Write out the dataframe as a parquet file.
df.coalesce(1).write                               \
    .format("delta")                               \
    .mode("append")                                \
    .save('/book/chapter02/multipleWriteOperations')


# Create the data as an array of tuples
data = [
    (8, 'Patient 8', 'San Diego')
]

# Create a dataframe from the above array and column
# definitions
df = spark.createDataFrame(data, columns)

# Write out the dataframe as a parquet file.
df.coalesce(1).write                                \
    .format("delta")                                \
    .mode("append")                                 \
    .save('/book/chapter02/multipleWriteOperations')