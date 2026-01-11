from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

# ==============================
# Initialize Spark Session
# ==============================
spark = SparkSession.builder \
    .appName("Traffic Processing") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .config("spark.hadoop.dfs.replication", "1") \
    .getOrCreate()

# Optional: reduce log verbosity
spark.sparkContext.setLogLevel("WARN")

# ==============================
# Read raw traffic data
# ==============================
raw_path = "hdfs://namenode:8020/data/raw/traffic"

df = spark.read \
    .option("recursiveFileLookup", "true") \
    .json(raw_path)

# Show schema (for debugging)
# df.printSchema()

# ==============================
# Compute Metrics
# ==============================

# 1. Average vehicle count per zone
avg_traffic_per_zone = df.groupBy("zone").agg(
    avg("vehicle_count").alias("avg_vehicle_count")
)

# 2. Average speed per road_id (route)
avg_speed_per_route = df.groupBy("road_id").agg(
    avg("average_speed").alias("avg_speed_kmh")
)

# 3. Average occupancy rate (congestion proxy)
avg_congestion = df.agg(
    avg("occupancy_rate").alias("avg_occupancy_rate_percent")
).withColumn("description", col("avg_occupancy_rate_percent"))

# Note: You could also do per-zone or per-road congestion if needed

# ==============================
# Write Results to HDFS
# ==============================
output_base = "hdfs://namenode:8020/data/processed/traffic"

# Save each result as Parquet (efficient + schema preserved)
avg_traffic_per_zone.write.mode("overwrite").parquet(f"{output_base}/by_zone")
avg_speed_per_route.write.mode("overwrite").parquet(f"{output_base}/by_route")
avg_congestion.coalesce(1).write.mode("overwrite").json(f"{output_base}/overall_congestion")

print("✅ Processing complete. Results saved to HDFS.")