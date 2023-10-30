from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("234") \
    .master("local[2]") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
    .config("hive.metastore.schema.verification", True) \
    .enableHiveSupport()\
    .getOrCreate()
schema = StructType([
    StructField("status", StringType(), True),
    StructField("day", StringType(), True),
    StructField("month", StringType(), True),
    StructField("date", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("code", StringType(), True),
    StructField("year", IntegerType(), True)
])
# base_df = spark.read.schema(schema_1).text("C:/Users/C ASISH KUMAR REDDY/OneDrive/Desktop/spark data/bigLog.txt")
# base_df.show()

base_df = spark.read.text("C:/Users/C ASISH KUMAR REDDY/OneDrive/Desktop/spark data/bigLog.txt")
#base_df.show(truncate=False)
split_data =base_df.withColumn("value", split("value", "[\ ,]"))

header=split_data.selectExpr("value[0] as status","value[1] as day","value[2] as month","value[3] as date","value[4] as time","value[5] as code","value[6] as year")
cleaning=header.withColumn("status",regexp_replace("status",":",""))
datatype = cleaning.withColumn("status",col("status").cast(StringType()))\
        .withColumn("date",col("date").cast(IntegerType())) \
    .withColumn("time", date_format("time","HH:ss:ss"))\
    .withColumn("month",lower(trim(col("month"))))

month_map={"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,"jul":7,"aug":8,"sep":9,"oct":10,
            "nov":11,"dec":12}
for month_name,month_number in month_map.items():
    datatype=datatype.withColumn("month",when(col("month")==month_name,month_number).otherwise(col("month"))) \
        .withColumn("date_1", concat(col("year"),lit('-'), col("month"),lit('-'), col("date"))) \
        .withColumn('date_1', to_date('date_1', 'yyyy-M-d'))
df=datatype.drop(col("day"),col("month"),col("date"),col("year"))
# datatype.show()
# df.show()
# df.printSchema()

# val = input("Enter your value: ")
# print(val)
# spark.sql("use default")
# spark.sql("show tables").show()
# df.write.mode("overwrite").format("parquet").partitionBy("status").option("path","D:/spark_output").save()

df.write.mode("overwrite").csv("D:/spark_output2/")