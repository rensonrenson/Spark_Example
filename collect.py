import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create the Spark SparkSession

spark = SparkSession.builder.appName('Assignment one').getOrCreate()

# Read csv file for data to convert as dataframe

df = spark.read.option("header",True).csv("res/user.csv")
dataCollect = df.collect()

# Data are collect to dataframe after using for loop to print the value

for row in dataCollect:
    print(row['location'] + "," +str(row['user_id']))
