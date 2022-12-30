import findspark
findspark.init()
from pyspark.sql import SparkSession

# Create the Spark SparkSession

spark = SparkSession.builder.appName('Assignment one').getOrCreate()

# Read csv file for data to convert as dataframe
user = spark.read.option("header",True).csv("res/transaction.csv")
transaction = spark.read.option("header",True).csv("res/transaction.csv")

# Union the two table
df= user.union(transaction)
df.show()
# Using Unionall to join table
df2 = user.unionAll(transaction)
df2.show()

