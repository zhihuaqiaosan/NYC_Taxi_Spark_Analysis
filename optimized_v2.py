import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col, avg, count

spark = SparkSession.builder \
    .appName("NYC_Taxi_Optimized_V2") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

print("=" * 50)
print("优化版本2 - 分区优化 + 缓存 + Kryo序列化")
print("=" * 50)

start_time = time.time()

# df = spark.read.parquet("yellow_tripdata_2024-01.parquet")
df = spark.read.parquet("yellow_tripdata_2024-*.parquet")

# 按上车时间添加小时列
df_with_hour = df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))

# 缓存中间结果（重复使用）
df_with_hour.cache()

print(f"数据加载并缓存完成")

# 分析1（使用缓存）
result1 = df_with_hour.groupBy("pickup_hour").agg(
    avg("tip_amount").alias("avg_tip"),
    avg("fare_amount").alias("avg_fare"),
    count("*").alias("trip_count")
).orderBy("pickup_hour")
result1.show(24)

# 分析2
result2 = df_with_hour.groupBy("PULocationID") \
    .agg(count("*").alias("trip_count")) \
    .orderBy(col("trip_count").desc()) \
    .limit(10)
result2.show()

# 分析3
result3 = df_with_hour.groupBy("passenger_count") \
    .agg(avg("tip_amount").alias("avg_tip"), count("*").alias("count")) \
    .orderBy("passenger_count")
result3.show()

# 释放缓存
df_with_hour.unpersist()

end_time = time.time()
print(f"优化版本2总耗时: {end_time - start_time:.2f} 秒")

spark.stop()