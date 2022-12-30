import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import split, col, count, hour, minute, second

spark=SparkSession.builder.appName("sparkbyexamples").getOrCreate()
header_schems = StructType([
    StructField("Logging", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("ghtorrent_id", StringType(), True),StructField("ghtorrent", StringType(), True)])

csvFiles = spark.read.option("header",True).schema(header_schems).csv("resources/logs.txt")


df1 = csvFiles.withColumn('ghtorrent',split(col("ghtorrent_id"),"--").getItem(0)) \
            .withColumn('api_client',split(col("ghtorrent_id"),"--").getItem(1)) \
            .withColumn('client',split(col("api_client"),":").getItem(0)) \
            .withColumn('req_client',split(col("api_client"),":").getItem(1)) \
            .withColumn('request', split(col("req_client"), ".").getItem(0))

df1.show(truncate=False)
#How many lines does the RDD contain?
total_line = df1.agg(count("*").alias("line_count"))
total_line.sort("line_count").show()
# #Count the number of WARNing messages
wran_msg = df1.filter("Logging = 'WARN'").agg(count("*").alias("wran_count"))
wran_msg.sort("wran_count").show()
# # How many repositories where processed in total? Use the api_client lines only.
total_rep_api = df1.filter(col("client").like("%api_client.rb%")).agg(count("*").alias("total_rep_api_count"))
total_rep_api.sort("total_rep_api_count").show()
# #Which client did most HTTP requests count
most_http_req = df1.groupBy("ghtorrent").agg(count("*").alias("most_http_req_count"))
most_http_req.sort("most_http_req_count").show()
# #Which client did most FAILED HTTP requests count
Failed_req = df1.filter(col("req_client").like("%Failed%")).groupBy("ghtorrent").agg(count("*").alias("Failed_req_count"))
Failed_req.sort("Failed_req_count").show()
#
# #7.	What is the most active hour of day
active_hour = df1.withColumn("hour", hour(col("timestamp"))).groupBy("hour").agg(count("*").alias("active_hour_count"))
active_hour.sort("active_hour_count").show(truncate=False)
#
# #What is the most active repository (hint: use messages from the ghtorrent.rb layer only)?
active_repository = df1.groupBy("ghtorrent").agg(count("*").alias("active_repository_count"))
active_repository.sort("active_repository_count").show(truncate=False)