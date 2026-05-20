import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col, avg, count, sum as _sum

# 创建 SparkSession（不做任何优化配置）
spark = SparkSession.builder \
    .appName("NYC_Taxi_Baseline") \
    .getOrCreate()

print("=" * 50)
print("基线版本 - 无任何优化")
print("=" * 50)

# 记录开始时间
start_time = time.time()

# 读取 parquet 文件
# df = spark.read.parquet("yellow_tripdata_2024-01.parquet")
df = spark.read.parquet("yellow_tripdata_2024-*.parquet")

print(f"数据加载完成，总行数: {df.count()}")

# 分析1：按小时统计平均小费和平均车费
print("\n>>> 分析1: 按小时统计平均小费")
df_hour = df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
result1 = df_hour.groupBy("pickup_hour").agg(
    avg("tip_amount").alias("avg_tip"),
    avg("fare_amount").alias("avg_fare"),
    count("*").alias("trip_count")
).orderBy("pickup_hour")
result1.show(24)

# 分析2：按上车区域统计Top10热门区域
print("\n>>> 分析2: Top10 最热门上车区域")
result2 = df.groupBy("PULocationID") \
    .agg(count("*").alias("trip_count")) \
    .orderBy(col("trip_count").desc()) \
    .limit(10)
result2.show()

# 分析3：按乘客数统计平均小费
print("\n>>> 分析3: 不同乘客数的平均小费")
result3 = df.groupBy("passenger_count") \
    .agg(avg("tip_amount").alias("avg_tip"), count("*").alias("count")) \
    .orderBy("passenger_count")
result3.show()

end_time = time.time()
print(f"\n基线版本总耗时: {end_time - start_time:.2f} 秒")

spark.stop()