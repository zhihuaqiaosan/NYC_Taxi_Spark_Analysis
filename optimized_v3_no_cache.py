import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col, avg, count

spark = SparkSession.builder \
    .appName("NYC_Taxi_Optimized_V3") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

print("=" * 50)
print("优化版本3 - 无缓存 + 内存限制 + AQE")
print("=" * 50)

start_time = time.time()

df = spark.read.parquet("yellow_tripdata_2024-*.parquet")
df_with_hour = df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
# 注意：去掉了 .cache() 这行

result1 = df_with_hour.groupBy("pickup_hour").agg(
    avg("tip_amount").alias("avg_tip"),
    avg("fare_amount").alias("avg_fare"),
    count("*").alias("trip_count")
).orderBy("pickup_hour")
result1.show(24)

result2 = df_with_hour.groupBy("PULocationID") \
    .agg(count("*").alias("trip_count")) \
    .orderBy(col("trip_count").desc()) \
    .limit(10)
result2.show()

result3 = df_with_hour.groupBy("passenger_count") \
    .agg(avg("tip_amount").alias("avg_tip"), count("*").alias("count")) \
    .orderBy("passenger_count")
result3.show()

end_time = time.time()
print(f"优化版本3总耗时: {end_time - start_time:.2f} 秒")

spark.stop()