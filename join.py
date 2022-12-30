import findspark
findspark.init()
from pyspark.sql import SparkSession

# Create the Spark SparkSession

spark = SparkSession.builder.appName('Assignment one').getOrCreate()

# Read csv file for data to convert as dataframe
user = spark.read.option("header",True).csv("res/transaction.csv")
transaction = spark.read.option("header",True).csv("res/transaction.csv")

# Join the two table using left ,right.inner,outer

# Join the two table using outer
user.join(transaction,user["product_id"] ==  transaction["product_id"],"outer").show(truncate=False)

# Join the two table using inner
user.join(transaction,user["product_id"] ==  transaction["product_id"],"inner").show(truncate=False)

# Join the two table using left
user.join(transaction,user["product_id"] ==  transaction["product_id"],"left").show(truncate=False)

# Join the two table using right
user.join(transaction,user["product_id"] ==  transaction["product_id"],"right").show(truncate=False)
