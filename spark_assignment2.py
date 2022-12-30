import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local").appName("hello spark").getOrCreate();
transaction_DF = spark.read.option("header",True).csv("resources/transaction.csv")
user_DF = spark.read.option("header",True).csv("resources/user.csv")
#Count of unique locations where each product is sold.
joinDF = user_DF.join(transaction_DF,user_DF.user_id == transaction_DF.userid,'fullouter')
joinDF.show()
count_Value = joinDF.groupBy("location","product_description").agg(count("*").alias("new_location"))
count_Value.show()
#Find out products bought by each user
product_bought= joinDF.groupBy("product_id","userid").agg(count("*").alias("new_count"))
product_bought.show()

#Total spending done by each user on each product
sumOf_price = joinDF.groupBy("product_id","userid").agg(sum("price").alias("sum_price"))
sumOf_price.show()





