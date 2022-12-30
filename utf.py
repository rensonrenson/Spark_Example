
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

# Create the Spark SparkSession

spark = SparkSession.builder.appName('Assignment one').getOrCreate()

# Read csv file for data to convert as dataframe
df = spark.read.option("header",True).csv("res/sample.txt")

def Converter(str):
    result = ""
    a = str.split(" ")

    for q in a:
        if q == 'A' or 'A' or 'R':
            result += q[1:2].upper()

    return result

NamechangeUDF = udf(lambda m: Converter(m))

df.withColumn("Special Names", NamechangeUDF("Name")).show()