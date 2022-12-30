import findspark
findspark.init()
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when

# Create the Spark SparkSession

spark = SparkSession.builder.appName('Assignment one').getOrCreate()

#Read csv file for data to convert as dataframe

df = spark.read.option("header",True).csv("res/user.csv")
# Add One Column to puls 1 for user id

df1 = df.withColumn("New_Column",col("user_id")+1)
df1.show()