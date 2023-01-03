import findspark
findspark.init()
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyspark import SparkConf
from  pyspark.sql import SparkSession


# here creat the Spark configation
conf = SparkConf().setMaster("local").setAppName("example for spark core")
# here create Spark Session
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext.getOrCreate(conf=conf)

#read the Csv Files
user = sc.textFile("res/user.csv")
transaction = sc.textFile("res/transaction.csv")
# Here Convert csv files to Rdd
rdd_user = user.map(lambda l: l.split(","))
rdd_transaction = transaction.map(lambda l: l.split(","))

print(rdd_user.collect())
print(rdd_transaction.collect())

# here convert Rdd to Dataframe
user_DF = rdd_user.toDF(schema =["user_id","email","nativelanguage","location"])
user_DF.show(truncate=False)
transaction_DF = rdd_transaction.toDF(schema=["transcation_id","product_id","userid","price","product_description"])
transaction_DF.show(truncate=False)
# here join the user and trasaction table
join_DF=user_DF.join(transaction_DF)
# sort the table value
ordreby_Df=join_DF.orderBy(join_DF.userid.asc())
ordreby_Df.printSchema()
# Count of unique locations where each product is sold
def unique_location(u):
    count_ul = ordreby_Df.groupBy("nativelanguage","product_description").count()
    return count_ul
# Find out products bought by each user.
u_l = unique_location(ordreby_Df)
u_l.show(truncate=False)
# Total spending done by each user on each product
def user_product(up):
     count = ordreby_Df.groupBy("userid","product_description").count()
     return count
up = user_product(ordreby_Df)
up.show(truncate=False)

def total_spent(sp):
    sp = ordreby_Df.withColumn("Price", col("price").cast(IntegerType()))
    return sp.groupBy("userid","product_description").sum("price")
sp=total_spent(ordreby_Df)
sp.show(truncate=False)
