import pyspark
from delta import *
from pyspark.sql.functions import to_date, col

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.read.format("csv")                                        \
               .option("inferschema", "true")                        \
               .option("header", "true")                             \
               .load("..\..\data\GreenTaxi\green_tripdata_2019-12.csv") 

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
df = df.withColumn('lpep_pickup_datetime', to_date(col('lpep_pickup_datetime'), 'yyy-MM-dd'))
df = df.withColumnRenamed("lpep_pickup_datetime", "pickupDate")
print(df)

df.write.partitionBy("pickupDate")       \
        .format("parquet")               \
        .save("/book/chapter02/partitionedData")
