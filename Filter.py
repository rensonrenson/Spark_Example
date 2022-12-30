import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import *

# Create the Spark SparkSession

spark = SparkSession.builder.appName('Assignment one').getOrCreate()

# write the schema for csv files
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("emailid", StringType(), True),
    StructField("nativelanguage", StringType(), True),
    StructField("location", StringType(), True)
])
# Read csv file for data to convert as dataframe
df = spark.read.option("header",True).schema(schema).csv("res/user.csv")
df.printSchema()

# filter the user id is 101 form dataframe

df.filter("user_id == '101'").show()
# Filter and where doing the same  
df.where("location == 'usa'").show()

