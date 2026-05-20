import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col, avg, count

spark = SparkSession.builder.appName("ExportForWeb").getOrCreate()

print("读取数据...")
df = spark.read.parquet("yellow_tripdata_2024-*.parquet")

# 1. 按小时聚合
print("处理按小时聚合...")
df_hour = df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
hour_stats = df_hour.groupBy("pickup_hour").agg(
    avg("tip_amount").alias("avg_tip"),
    avg("fare_amount").alias("avg_fare"),
    count("*").alias("trip_count")
).orderBy("pickup_hour").collect()

hour_data = {
    "hours": [row["pickup_hour"] for row in hour_stats],
    "avg_tip": [float(row["avg_tip"]) for row in hour_stats],
    "avg_fare": [float(row["avg_fare"]) for row in hour_stats],
    "trip_count": [row["trip_count"] for row in hour_stats]
}

# 2. Top10热门区域
print("处理热门区域...")
top_locations = df.groupBy("PULocationID") \
    .agg(count("*").alias("trip_count")) \
    .orderBy(col("trip_count").desc()) \
    .limit(10) \
    .collect()

location_data = {
    "location_ids": [str(row["PULocationID"]) for row in top_locations],
    "trip_counts": [row["trip_count"] for row in top_locations]
}

# 3. 乘客数统计
print("处理乘客数统计...")
passenger_stats = df.groupBy("passenger_count") \
    .agg(avg("tip_amount").alias("avg_tip"), count("*").alias("count")) \
    .orderBy("passenger_count") \
    .collect()

passenger_data = {
    "counts": [row["passenger_count"] if row["passenger_count"] is not None else 0 for row in passenger_stats],
    "avg_tip": [float(row["avg_tip"]) for row in passenger_stats],
    "trip_count": [row["count"] for row in passenger_stats]
}

# 4. 整体统计
print("处理整体统计...")
total_trips = df.count()
avg_tip = df.select(avg("tip_amount")).collect()[0][0]
avg_fare = df.select(avg("fare_amount")).collect()[0][0]
avg_distance = df.select(avg("trip_distance")).collect()[0][0]

summary_data = {
    "total_trips": total_trips,
    "avg_tip": float(avg_tip),
    "avg_fare": float(avg_fare),
    "avg_distance": float(avg_distance)
}

# 创建目录并保存
os.makedirs("web/static", exist_ok=True)

with open("web/static/hour_data.json", "w") as f:
    json.dump(hour_data, f)
with open("web/static/location_data.json", "w") as f:
    json.dump(location_data, f)
with open("web/static/passenger_data.json", "w") as f:
    json.dump(passenger_data, f)
with open("web/static/summary_data.json", "w") as f:
    json.dump(summary_data, f)

print("✅ 导出完成！文件保存在 web/static/ 目录")
spark.stop()