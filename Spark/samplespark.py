from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = SparkSession.builder \
    .appName("234") \
    .master("local[2]") \
    .getOrCreate()

base_df = spark.read.csv("C:/Users/C ASISH KUMAR REDDY/OneDrive/Desktop/spark data/customersdata.csv", header=True, inferSchema=True)

final_df = base_df.groupBy("customer_id").agg(count("customer_id"))

print(final_df.count())

print(final_df.rdd.getNumPartitions())

final_df.show()

val = input("Enter your value: ")
print(val)

spark.sql("use default")
spark.sql("show tables").show()
