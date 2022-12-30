import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create the Spark SparkSession

spark = SparkSession.builder.appName('Assignment one').getOrCreate()

#Read csv file for data to convert as dataframe

df = spark.read.option("header",True).csv("res/user.csv")
# Select All column value
df3 = df.select("*")
df3.show()
# select the column name to select value
df1 = df.select(df["user_id"],df["nativelanguage"])
df1.show()
# select the column name to select as new column
df2= df.select(col("user_id").alias("new_user_id"),col("nativelanguage").alias("new_nativelanguage"))
df2.show()