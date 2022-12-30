import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark import StorageLevel
# Create the Spark SparkSession

spark = SparkSession.builder.appName('Assignment one').getOrCreate()

# Read csv file for data to convert as dataframe
df = spark.read.option("header",True).csv("res/transaction.csv")

dfPersist = df.persist()
dfPersist.show()

dfPersist = df.persist(StorageLevel.MEMORY_ONLY)
dfPersist.show()
