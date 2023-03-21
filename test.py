from pyspark.sql import *
spark = SparkSession.builder.appName("sdd").getOrCreate()
def cdf():
    df = spark.createDataFrame([(1, "brazil"), (2, "us"), (2, "india2")],["id", "country"] )
    # df.show()
    return df