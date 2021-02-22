__author__ = "Mahateesh Rao Venepally"
__purpose__ = "interview"

# importing the libraries
import findspark

findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import Window

# creating a spark session
spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("My Test") \
    .getOrCreate()

# reading the country data
countryData = spark \
    .read \
    .format("csv") \
    .option("header", "true") \
    .load("../../data/countrylist.csv")

# reading the station data
stationData = spark \
    .read \
    .format("csv") \
    .option("header", "true") \
    .load("../../data/stationlist.csv")

# reading temperature data
tempData = spark \
    .read \
    .format("csv") \
    .option("header", "true") \
    .load("/Users/sahilnagpal/Downloads/paytmteam-de-weather-challenge-beb4fc53605c/data/2019/*.csv.gz")

print("task -1")
print("The country with maximum Temperature  : DJIBOUTI | 90.06114457831325 ")

# renaming the column
countryData = countryData.withColumnRenamed("COUNTRY_ABBR", "COUNTRY_ABBR_country")

# creating the expression
exprss = countryData.COUNTRY_ABBR_country == stationData.COUNTRY_ABBR

# defining the join type
joinType = "inner"

# joining the data
country_station = countryData.join(stationData, exprss, joinType)
country_station = country_station.select("COUNTRY_FULL", "STN_NO", "COUNTRY_ABBR")
tempData = tempData.withColumnRenamed("STN---", "STN")
exprss1 = country_station.STN_NO == tempData.STN
joineddata = country_station.join(tempData, exprss1, joinType)

# creating the solution
joineddata \
    .groupBy(col("COUNTRY_FULL")) \
    .agg({'TEMP': 'mean'}) \
    .orderBy(col("avg(TEMP)").desc()) \
    .show(10)

print("task -3")
print("The country with maximum wind speed  : GABON | 485.17947805456754 ")

# creating the solution
joineddata \
    .groupBy(col("COUNTRY_FULL")) \
    .agg({'WDSP': 'mean'}) \
    .orderBy(col("avg(WDSP)").desc()) \
    .show(10)

print("task -2")
joineddata.createOrReplaceTempView("joineddata")

spark.sql('''WITH
 
  -- This table contains all the distinct date 
  -- instances in the data set
  dates(YEARMODA) AS (
    SELECT DISTINCT CAST(YEARMODA AS DATE)
    FROM joineddata
    WHERE substring("FRSHTT",6,1) = 1
  ),
   
  groups AS (
    SELECT
      ROW_NUMBER() OVER (ORDER BY YEARMODA) AS rn,
      dateadd(day, -ROW_NUMBER() OVER (ORDER BY YEARMODA), YEARMODA) AS grp,
      YEARMODA
    FROM joineddata
  )
SELECT *
FROM groups
ORDER BY rn''').show()

input("")
