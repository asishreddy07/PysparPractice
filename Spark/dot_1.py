from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder \
    .appName("234") \
    .master("local[2]") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
    .config("hive.metastore.schema.verification", True) \
    .enableHiveSupport()\
    .getOrCreate()
Base_df=spark.read.text