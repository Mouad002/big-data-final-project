# from pyspark.sql import SparkSession
# from pyspark.sql.functions import avg, col

# # ==============================
# # Initialize Spark Session
# # ==============================
# spark = SparkSession.builder \
#     .appName("Traffic Processing") \
#     .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
#     .config("spark.hadoop.dfs.replication", "1") \
#     .getOrCreate()

# # Optional: reduce log verbosity
# spark.sparkContext.setLogLevel("WARN")

# # ==============================
# # Read raw traffic data
# # ==============================
# raw_path = "hdfs://namenode:8020/data/raw/traffic"

# df = spark.read \
#     .option("recursiveFileLookup", "true") \
#     .json(raw_path)

# # Show schema (for debugging)
# # df.printSchema()

# # ==============================
# # Compute Metrics
# # ==============================

# # 1. Average vehicle count per zone
# avg_traffic_per_zone = df.groupBy("zone").agg(
#     avg("vehicle_count").alias("avg_vehicle_count")
# )

# # 2. Average speed per road_id (route)
# avg_speed_per_route = df.groupBy("road_id").agg(
#     avg("average_speed").alias("avg_speed_kmh")
# )

# # 3. Average occupancy rate (congestion proxy)
# avg_congestion = df.agg(
#     avg("occupancy_rate").alias("avg_occupancy_rate_percent")
# ).withColumn("description", col("avg_occupancy_rate_percent"))

# # Note: You could also do per-zone or per-road congestion if needed

# # ==============================
# # Write Results to HDFS
# # ==============================
# output_base = "hdfs://namenode:8020/data/processed/traffic"

# # Save each result as Parquet (efficient + schema preserved)
# avg_traffic_per_zone.write.mode("overwrite").parquet(f"{output_base}/by_zone")
# avg_speed_per_route.write.mode("overwrite").parquet(f"{output_base}/by_route")
# avg_congestion.coalesce(1).write.mode("overwrite").json(f"{output_base}/overall_congestion")











# ==============================















# # In your Spark script (process_traffic.py)

# Parse event_time and extract date
from pyspark.sql.functions import col, avg, to_date

df = spark.read.option("recursiveFileLookup", "true").json("hdfs://namenode:8020/data/raw/traffic")

# Add date column
df_with_date = df.withColumn("event_date", to_date(col("event_time")))

# 1. Traffic per zone per day
by_zone_date = df_with_date.groupBy("event_date", "zone").agg(
    avg("vehicle_count").alias("avg_vehicle_count")
)

# 2. Speed per route per day
by_route_date = df_with_date.groupBy("event_date", "road_id").agg(
    avg("average_speed").alias("avg_speed_kmh")
)

# 3. Congestion per zone per day
by_congestion_date = df_with_date.groupBy("event_date", "zone").agg(
    avg("occupancy_rate").alias("avg_occupancy_rate")
)

# Save with date partitioning
by_zone_date.write \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .parquet("hdfs://namenode:8020/data/processed/traffic/by_zone_date")

by_route_date.write \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .parquet("hdfs://namenode:8020/data/processed/traffic/by_route_date")

by_congestion_date.write \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .parquet("hdfs://namenode:8020/data/processed/traffic/by_congestion_date")









# ============================== a working one







# # traffic_processor.py (updated for Grafana compatibility)

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import avg, col, current_timestamp, date_trunc, lit
# from datetime import datetime

# spark = SparkSession.builder \
#     .appName("Traffic Processing for Grafana") \
#     .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
#     .config("spark.hadoop.dfs.replication", "1") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # Read raw data
# raw_path = "hdfs://namenode:8020/data/raw/traffic"
# df = spark.read.option("recursiveFileLookup", "true").json(raw_path)

# # Parse event_time as timestamp
# from pyspark.sql.types import TimestampType
# df = df.withColumn("event_time", col("event_time").cast(TimestampType()))

# # Optional: truncate to hour for stable aggregation window
# df = df.withColumn("event_hour", date_trunc("hour", col("event_time")))

# # Compute KPIs per zone and hour
# kpi_by_zone = df.groupBy("zone", "event_hour").agg(
#     avg("vehicle_count").alias("avg_vehicle_count"),
#     avg("average_speed").alias("avg_speed_kmh"),
#     avg("occupancy_rate").alias("avg_occupancy_rate")
# ).withColumn("processing_time", current_timestamp())

# # Also compute overall congestion (optional)
# overall = df.agg(
#     avg("occupancy_rate").alias("avg_occupancy_rate")
# ).withColumn("processing_time", current_timestamp()).withColumn("zone", lit("ALL"))

# # Write analytics zone (Parquet - efficient & schema-preserving)
# analytics_base = "hdfs://namenode:8020/data/analytics/traffic"

# kpi_by_zone.write.mode("overwrite").parquet(f"{analytics_base}/by_zone")
# overall.coalesce(1).write.mode("overwrite").json(f"{analytics_base}/overall")

# print("✅ Grafana-ready KPIs saved to /data/analytics/traffic")
# spark.stop()




# ==============================

# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
# from pyspark.sql.functions import avg, col, current_timestamp, date_trunc, lit

# # Schema for raw data
# schema = StructType([
#     StructField("sensor_id", StringType(), True),
#     StructField("road_id", StringType(), True),
#     StructField("road_type", StringType(), True),
#     StructField("zone", StringType(), True),
#     StructField("vehicle_count", IntegerType(), True),
#     StructField("average_speed", DoubleType(), True),
#     StructField("occupancy_rate", IntegerType(), True),
#     StructField("event_time", StringType(), True)
# ])

# spark = SparkSession.builder \
#     .appName("Traffic Processing for Grafana") \
#     .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
#     .config("spark.hadoop.dfs.replication", "1") \
#     .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # === Read raw data ===
# raw_path = "hdfs://namenode:8020/data/raw/traffic"
# df = spark.read.option("recursiveFileLookup", "true").schema(schema).json(raw_path)

# if df.isEmpty():
#     print("⚠️ No data found in raw zone. Exiting.")
#     spark.stop()
#     exit(0)

# df = df.withColumn("event_time", col("event_time").cast("timestamp"))
# df = df.withColumn("event_hour", date_trunc("hour", col("event_time")))

# # === Compute KPIs per zone/hour ===
# kpi_by_zone = df.groupBy("zone", "event_hour").agg(
#     avg("vehicle_count").alias("avg_vehicle_count"),
#     avg("average_speed").alias("avg_speed_kmh"),
#     avg("occupancy_rate").alias("avg_occupancy_rate")
# ).withColumn("processing_time", current_timestamp())

# # === Save to HDFS (Analytics Zone - Parquet) ===
# analytics_base = "hdfs://namenode:8020/data/analytics/traffic"
# kpi_by_zone.write.mode("overwrite").parquet(f"{analytics_base}/by_zone")
# print("✅ Saved analytics to HDFS.")

# # === Save to PostgreSQL ===
# kpi_by_zone.write \
#     .format("jdbc") \
#     .option("url", "jdbc:postgresql://postgres:5432/smartcity") \
#     .option("dbtable", "traffic_kpis") \
#     .option("user", "grafana") \
#     .option("password", "grafana") \
#     .option("driver", "org.postgresql.Driver") \
#     .mode("append") \
#     .save()

# print("✅ Appended KPIs to PostgreSQL.")
# spark.stop()



print("✅ Processing complete. Results saved to HDFS.")
