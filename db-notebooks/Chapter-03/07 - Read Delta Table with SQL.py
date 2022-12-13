# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />   
# MAGIC 
# MAGIC  Name:          chapter 03/07 - Read Delta Table with SQL
# MAGIC  
# MAGIC      Author:    Bennie Haelen
# MAGIC      Date:      12-10-2022
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 3 of the book - Basic Operations on Delta Tables.
# MAGIC                 This notebook illustrates how to read a table using SQL
# MAGIC 
# MAGIC                 
# MAGIC      The following Delta Lake functionality is demonstrated in this notebook:
# MAGIC        1 - Create a Delta Table on top of a Delta File
# MAGIC        2 - Perform a record count with SQL
# MAGIC        3 - Perform a DESCRIBE FORMATTED listing of the Delta table
# MAGIC        4 - Illustrate the use of ANSI SQL in a more advanced query
# MAGIC        5 - Demonstrate the usage of Spark SQL in Python

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS taxidb.YellowTaxis;

# COMMAND ----------

# MAGIC %md
# MAGIC ###1 - Create a Delta Table on top of a Delta File

# COMMAND ----------

# MAGIC %sh
# MAGIC # This is our Delta file created by the "00 - Chapter Initialization" notebook
# MAGIC ls -al /dbfs/mnt/datalake/book/chapter03/YellowTaxisDelta/

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Syntax to create a Delta Table on top of an existing
# MAGIC -- Delta File
# MAGIC CREATE TABLE taxidb.YellowTaxis
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/datalake/book/chapter03/YellowTaxisDelta/"

# COMMAND ----------

# MAGIC %md
# MAGIC ###2- Quick record count with SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COUNT(*)
# MAGIC FROM
# MAGIC     taxidb.yellowtaxis

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - Perform a DESCRIBE FORMATTED of the table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE FORMATTED taxidb.YellowTaxis;

# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - Illustrate the use of ANSI SQL in a more advanced query

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Demonstrate the Spark SQL supports ANSI-SQL constructs
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

# MAGIC %md
# MAGIC ###5 - Demonstrate the use of spark.sql in Python

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
