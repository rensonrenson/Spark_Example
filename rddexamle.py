import findspark
findspark.init()
from pyspark.sql import SparkSession

# Create the Spark SparkSession

spark = SparkSession.builder.appName('Assignment one').getOrCreate()

user = spark.sparkContext.textFile("res/user.csv").map(lambda line: line.split(",")) \
#     .collect()
#
#print(user)

transaction = spark.sparkContext.textFile("res/transaction.csv").map(lambda line: line.split(",")) \
#     .collect()
#print(transaction)
userTransactionRDD=user.fullOuterJoin(transaction)
dataColl=userTransactionRDD.collect()
for row in dataColl:
    print(row)
#userTransactionRDD.show()
#print(userTransactionRDD)