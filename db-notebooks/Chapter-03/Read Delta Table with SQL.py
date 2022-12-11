# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE taxidb.YellowTaxis;

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "dbfs:/mnt/datalake/book/Data Files/YellowTaxisParquet"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE taxidb.YellowTaxis
# MAGIC USING PARQUET
# MAGIC LOCATION "/mnt/datalake/book/Data Files/YellowTaxisParquet/"

# COMMAND ----------

df = spark.sql("select * from taxidb.YellowTaxis")
df.write.format("delta").save("/mnt/datalake/book/chapter03/YellowTaxisDelta/")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE taxidb.YellowTaxis;

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "dbfs:/mnt/datalake/book/chapter03/YellowTaxisDelta/"

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter03/YellowTaxisDelta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE taxidb.YellowTaxis
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/datalake/book/chapter03/YellowTaxisDelta/"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COUNT(*)
# MAGIC FROM
# MAGIC     taxidb.yellowtaxis

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC SELECT COUNT(*) from taxidb.YellowTaxis

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE FORMATTED taxidb.YellowTaxis;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CabNumber,
# MAGIC     AVG(FareAmount) AS AverageFare
# MAGIC FROM
# MAGIC     taxidb.yellowtaxis
# MAGIC GROUP BY
# MAGIC     CabNumber
# MAGIC HAVING
# MAGIC      AVG(FareAmount) > 50
# MAGIC ORDER BY
# MAGIC     2 DESC
# MAGIC LIMIT 5

# COMMAND ----------

number_of_results = 5

sql_statement = f"""
SELECT 
    CabNumber,
    AVG(FareAmount) AS AverageFare
FROM
    taxidb.yellowtaxis
GROUP BY
    CabNumber
HAVING
     AVG(FareAmount) > 50
ORDER BY
    2 DESC
LIMIT {number_of_results}"""

df = spark.sql(sql_statement)
display(df)
