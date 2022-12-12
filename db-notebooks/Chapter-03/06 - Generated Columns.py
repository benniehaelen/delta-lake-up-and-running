# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: right; margin: 0px 15px 15px 0px;" src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAATEAAAClCAMAAAADOzq7AAAAn1BMVEX///8tJyZDsdjY19cqIyJFQEAlHh0TBQDb2trPzs7x8fH29vbi4eGVkpFxbW08r9cYDw00Li0GAAC5uLh3dHMgGBcwKihTTk2AfX1PSUkdFRPx+fz6+fk/OjkRAAAlHx1PttqhnZ03MTC03u51xOHB5PHFxMRgXFvi8/mMzudsv9+d0+nn5+empKTP6vWzsbCIhYWo2+1lYWCAx+To9vvgu+s0AAAJI0lEQVR4nO2biZKqOhCGXSLHBUQHd1BR3EDF9f2f7UL2QHB07qnyzrW/mipnIBu/nU6nw5RKAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACU1tHFffcYfhU7367E7x7Eb8IN7Yod3d89jF9EXEmwg3cP4/fgruxUMX/37oH8GoJUsESy/bsH8lvY+RUKOP/nCG0qmL2CCOMZ4grHvrx7ML8B4vYpPhjZ9wQVmfDdw/nvc48kE0siDHD+3xFWVG7vHtB/ndhXBbMr4Pwf4t7sjI1VIoj8HxH4PBbjkkHk/4CdiCzE7IweOf9DlVM3D57+DqOVXPZM/GuTFWvVc8Xq7GaznnIo6ruJS5utgru4silfUvtumXXefm6wDx5Z4cJlWgU37srCB0FZb1pjjLej44Y/XssSdyjTTXK9vsW/Hlm5xjZbrDY+0SbO45S2meuV4OAetg3tzYM1zFU2r1PRd/O4xc2PreT3c2awy6fkStx+xIOKtfpHIVMDcQxjsuw59BtstvsoQ2eRKoQr9HusgZPcAmFiEVvdzEg1y9P1XCpZuIeBo73pLOe49lm6Vkeib88hHRvp92hmBzt7TjB3z+dkalZ7bmSr4txiDZVl0LxmHahiRjnDBCuGKxgjoRjKljMWRKIraQHVmtquSxa+39Uq1jzTysO6pNgAN48Va2znpLfUoM3sYDvPKbZmbp/ErVIsW5xbrGWfFw1Gzb+jWH1C/56fXldswwa2PIqLkmIHOjxiwD9U7C7cPlke+cJpF0cYOcXKCFmtv6JYr89aHL+sWGvBK7fFyiEUa5G6ZWOL140fKnbhgQVLvnIJi50/Vazf6XQmBpPMkRSb/OF8HTWKOd1ZcqtDZUtLz2apsiVPKNnRO/cHitWHrDaqiftCMQfh+2hAJryZGezs6xnB7iKeYJMw5lGZXWRkRLHu2XGc48jo0y/O44oZx4YgNZ0qYqMmeNX0jjNGxArTP8jyb4mv3bi+qJh3mojKZ75wcMXMJRGsTKMIqthUDPYZxUQeMdJcK8otYsXQkPTs9chTfjW4YoNstJRVjNAkNb+k0mNptv6plzQUK3bYCrnReMMuE8XmvdIUe31ksKpEMTQuCO30SHlEEbG6YgtQ4PypYvQ7aZEHT/Vgs/LQYpCv+rFiYlU8dSW3YpxLGooVa3Skyv0Fu8xsbETmAlowhZhiJh/sd3IlRMJnSVcl36aPMFTFShti7VMxK68jBhnfk4p5V3lBQFNd4F+oWOs6l+W+sgieKFbukg/U440yP7ZlY+3l2swRiO2RIo1eSEFGMZN43I6wMYPRN/AAn1SMRgfLLfkcHEt5ChWr01istySfR09RLCukUIyP9XvPfy+aftz5F7xVkFGMhjlfuugCvaCYd8ZV5ltnhG+gq2aeFCp2xrMO1Q5X6iQOecXQcCMq/CC62Be6eOH8bzrnn1WMPrn3LxVrEFsdWN4J2wmaarxVkWLNAa6c+C+6ZLIiimLy3ut1xWK/0JJ2+aDjkWJ/ycYW2O+ny5xJfBLq5TeXRYodJ0TkaqlJwuM+2YaoivUk1/i6YrcHoWogog6N89f7sYlQLNmfz+fpj/GCHzPbZAUZJTJZpJlxPkQqUMyr0SA5aYyE/mjayClWHpyFZEwxNKeD/c6PBeqGUuV+y2yeHinWyK6VaNtmWM+vlScSYZa3p9OpTR/znDOyAsUcMhXn7ePpRAPEPulbUQx1Lb7OMMWGbKz6kFlosmKSaE901/yuJreYiceInzbaQjEe4jSfj8dY4qE8WC6XNC4ztrkotkAxmvIod0VlGp1QxZY0HCsv2JdA47GaGOxjxXhaJyEKc6q4oZIE0ipGF2qLjEWK+bsFMf9Ivaoqtsnv73UBhl6xxiBftzw5CcXmPeeLSGaw2JYqNnwy5o/VE0q7sr9nC/AwNuf8adiU7gcdazqh5iDtKy1HIPaViVsSNDOKNc/93COn8VPWyIhi6LzhLdWTR+5p1E7Mp1WS9pUsszE7yoqVl3ywBfklgmRCQjN14y3lFrM7cpa7SPMPfUS/ujTSYZ4/zWpQcO6iSgt1REqjnVGsOtQ+dC7AYBkb3tTsj1Oq6+yTpj9E7sKihWaOrJgY7EPPz/OIsmZ+IM8/cWCSc3Qv58eq+XTYVVXMO1ILQAMCLdc/Z7yLlesh2VpbdAuEyrQy6W++VRRrnUm6p/xnIysmBH4gGFsK7UQnm9lY8hPK1iTlFjNuLp+DXY6wl/25YiZLPJwtwnlKWp5u1L51ih22ZNbXRqwyHaFRVXOwIzZc51XFAqyR7fvRLdzvL5fLPlxFfnJlJWnjioOlver8s3l+Y/pNnv9bxTyHTjaxnC7o1LdUz5xXDDlHErMa3B69I51wIzXPb1J/h2qb1xRL3H4i12ofxPGdauHu4vV+5duRdIQUF0VsyllSv//sWZJMHyuGS6eKsYoz4eibXXLsM1Z9v5XrAS3auORcSryaQ1xsvjwoZ0mlOu3HSDcWL5wlBbYfBvEuGzW4u/Xel7eYRdvL0avnlWbufHK6SPPyuHSa0jvgQ8Xa1JJ6OdEe1INXJ9fD9kjOT8ciOC21TkNys5rsuOSz0mqP9n/2XjmvjC8xV8B1766Qw41la9oVHCwpZ+LN58/EZdKnI+fbdamA7OY92oM6K1ua03RTPVXHxeqs98x5PBuh6f3kTDyxqdsqiqJVuA+0Of2LSKDBe4slN1j5PlkkU0X8le5/kkRu8eNfXXEvPo8sWHjhh7k8RcxLFB4sfQhJBJsLYXV7JSklFGkb+hDcfU4tposa9z/IbH8Uu/w7iZKdRYEcdojX1u3oY50/2zDiLRJFtbNwvdslMYd73+3k19Y/1cjouymJo49W6R5pH95W6SqgLgIRvhMp7u5Tl8sb3nNH4WXNov5khxSEajJDa3yf+gJ2uqUM13FmUbzHl0izfCqT9VPji8S1xzof7u6Ch5p97loZFL+t+UCzh+/FfjLuWquZXbnBf9wXgpNnmUXg9qku7Enue58tl8mnvwo+NnR9gfgSkjTQZQ1yAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMD/j38AFGDXktloRdIAAAAASUVORK5CYII=" />   
# MAGIC 
# MAGIC  Name:          chapter 03/06 - Generated Columns
# MAGIC  
# MAGIC      Author:    Bennie Haelen
# MAGIC      Date:      12-10-2022
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 3 of the book - Basic Operations on Delta Tables.
# MAGIC                 This notebook illustrates how to use the GENERATED COLUMNS feature of Delta Lake
# MAGIC 
# MAGIC                 
# MAGIC      The following Delta Lake functionality is demonstrated in this notebook:
# MAGIC        1 - Create a table with GENERATE ALWAYS AS columns
# MAGIC        2 - Insert a rows in the table, triggering the computation of the GENERATED columns
# MAGIC        3 - Perform a SELECT to illustrate that the GENERATED ALWAYS AS column 
# MAGIC        4 - An example of how you cannot use a non-deterministic function to calculate a GENERATED column

# COMMAND ----------

# MAGIC %md
# MAGIC ###1 - Create a simple version of the YellowTaxis table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Note that we use GENERATED ALWAYS AS columns to calculate the
# MAGIC -- PickYear, PickupMonth and Pickup Day
# MAGIC CREATE OR REPLACE TABLE taxidb.YellowTaxis
# MAGIC (
# MAGIC     RideId               INT        COMMENT 'This is our primary Key column',
# MAGIC     VendorId             INT,
# MAGIC     PickupTime           TIMESTAMP,
# MAGIC     PickupYear           INT        GENERATED ALWAYS AS(YEAR  (PickupTime)),
# MAGIC     PickupMonth          INT        GENERATED ALWAYS AS(MONTH (PickupTime)),
# MAGIC     PickupDay            INT        GENERATED ALWAYS AS(DAY   (PickupTime)),
# MAGIC     DropTime             TIMESTAMP,
# MAGIC     CabNumber            STRING     COMMENT 'Official Yellow Cab Number'
# MAGIC ) USING DELTA
# MAGIC LOCATION "/mnt/datalake/book/chapter03/YellowTaxis.delta"
# MAGIC COMMENT 'Table to store Yellow Taxi data'

# COMMAND ----------

# MAGIC %md
# MAGIC ###2 - Insert a record in the table, this will trigger the computation of the GENERATED columns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert a record, triggering the calculation of our GENRATED columns
# MAGIC INSERT INTO taxidb.YellowTaxis
# MAGIC     (RideId, VendorId, PickupTime, DropTime, CabNumber)
# MAGIC VALUES
# MAGIC     (5, 101, '2021-7-1T8:43:28UTC+3', '2021-7-1T8:43:28UTC+3', '51-986')

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - Perform a select of the relevant columns to ensure that our GENERATED columns are correct

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Illustrate that our GENERATED columns were calculated correctly
# MAGIC SELECT PickupTime, PickupYear, PickupMonth, PickupDay FROM taxidb.YellowTaxis

# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - An example of an invalid GENERATED ALWAYS AS function - UUID's are non-deterministic

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Here, we are trying to create a table that has
# MAGIC -- a GUID primary key
# MAGIC CREATE OR REPLACE TABLE default.dummy
# MAGIC (
# MAGIC     ID   STRING GENERATED ALWAYS AS (UUID()),
# MAGIC     Name STRING
# MAGIC ) USING DELTA
