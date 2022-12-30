import findspark
findspark.init()
from pyspark.sql import SparkSession

# Create the Spark SparkSession

spark = SparkSession.builder.appName('Assignment one').getOrCreate()

#Read csv file for data to convert as dataframe

df = spark.read.option("header",True).csv("res/user.csv").show()

