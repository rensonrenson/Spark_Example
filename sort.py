import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import *

# Create the Spark SparkSession

spark = SparkSession.builder.appName('Assignment one').getOrCreate()

# write the schema for csv files
schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("userid", StringType(), True),
    StructField("price", IntegerType(), True),
    StructField("product_description", StringType(), True)
])
# Read csv file for data to convert as dataframe
df = spark.read.option("header",True).schema(schema).csv("res/transaction.csv")
df.printSchema()
# Sort the dataframe
df.sort("price").show()

# Using orderby using to order the price value
df.orderBy(col("product_description")).show()
