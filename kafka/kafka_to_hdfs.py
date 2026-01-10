import json
import time
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError
from hdfs import InsecureClient
from hdfs.util import HdfsError

# =====================
# Configuration
# =====================
KAFKA_BROKER = "kafka:29092"
TOPIC = "traffic-events"
GROUP_ID = "traffic-hdfs-consumer"

HDFS_URL = "http://namenode:9870"
HDFS_BASE_PATH = "/data/raw/traffic"
HDFS_USER = "hdfs"

RETRY_DELAY = 5  # seconds

# =====================
# Clients
# =====================
hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)
consumer = None  # 👈 important

print("📥 Kafka → HDFS consumer starting...")

while True:
    try:
        # =====================
        # Initialize consumer only once
        # =====================
        if consumer is None:
            print("⏳ Initializing Kafka consumer...")
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id=GROUP_ID
            )
            print("✅ Kafka consumer initialized")

        # =====================
        # Consume messages
        # =====================
        for message in consumer:
            event = message.value

            try:
                event_time = datetime.fromisoformat(event["event_time"])
                date_str = event_time.strftime("%Y-%m-%d")
                zone = event["zone"]

                hdfs_dir = f"{HDFS_BASE_PATH}/date={date_str}/zone={zone}"
                hdfs_file = f"{hdfs_dir}/traffic.json"

                hdfs_client.makedirs(hdfs_dir)

                if not hdfs_client.status(hdfs_file, strict=False):
                    with hdfs_client.write(hdfs_file, encoding="utf-8") as writer:
                        writer.write(json.dumps(event) + "\n")
                else:
                    with hdfs_client.write(
                        hdfs_file, append=True, encoding="utf-8"
                    ) as writer:
                        writer.write(json.dumps(event) + "\n")

            except HdfsError as e:
                print(f"⚠ HDFS error: {e}")
                time.sleep(RETRY_DELAY)

            except Exception as e:
                print(f"🔥 Processing error: {e}")
                time.sleep(RETRY_DELAY)

    except NoBrokersAvailable:
        print("❌ Kafka not ready, retrying in 5s...")
        consumer = None  # 👈 force re-init
        time.sleep(RETRY_DELAY)

    except KafkaError as e:
        print(f"⚠ Kafka error: {e}, retrying in 5s...")
        consumer = None  # 👈 force re-init
        time.sleep(RETRY_DELAY)

    except Exception as e:
        print(f"🔥 Unexpected error: {e}")
        consumer = None
        time.sleep(RETRY_DELAY)


# import json
# from kafka import KafkaConsumer
# import time
# from hdfs import InsecureClient
# from kafka.errors import NoBrokersAvailable, KafkaError
# from datetime import datetime
# from hdfs.util import HdfsError

# # Kafka config
# KAFKA_BROKER = "kafka:29092"
# TOPIC = "traffic-events"
# GROUP_ID = "traffic-hdfs-consumer"

# # HDFS config
# HDFS_URL = "http://namenode:9870"
# HDFS_BASE_PATH = "/data/raw/traffic"
# HDFS_USER = "hdfs"

# RETRY_DELAY = 5  # seconds

# while True:
#     try:
#         consumer = KafkaConsumer(
#             TOPIC,
#             bootstrap_servers=KAFKA_BROKER,
#             value_deserializer=lambda m: json.loads(m.decode("utf-8")),
#             auto_offset_reset="earliest",
#             enable_auto_commit=True,
#             group_id=GROUP_ID
#         )
#         break
#     except NoBrokersAvailable:
#         print("❌ Kafka not ready, retrying in 5s...")
#         time.sleep(5)

#     except KafkaError as e:
#         print(f"⚠ Kafka error: {e}, retrying in 5s...")
#         time.sleep(5)

#     except Exception as e:
#         print(f"🔥 Unexpected error: {e}")
#         time.sleep(5)

# hdfs_client = InsecureClient(HDFS_URL, user="hdfs")

# print("📥 Kafka → HDFS consumer started...")

# for message in consumer:
#     event = message.value

#     event_time = datetime.fromisoformat(event["event_time"])
#     date_str = event_time.strftime("%Y-%m-%d")
#     zone = event["zone"]

#     hdfs_dir = f"{HDFS_BASE_PATH}/date={date_str}/zone={zone}"
#     hdfs_file = f"{hdfs_dir}/traffic.json"

#     # Create directory if it doesn't exist
#     hdfs_client.makedirs(hdfs_dir)

#     # Append event as JSON line
#     try:
#         if not hdfs_client.status(hdfs_file, strict=False):
#             # File does not exist → create it
#             with hdfs_client.write(hdfs_file, encoding="utf-8") as writer:
#                 writer.write(json.dumps(event) + "\n")
#         else:
#             # File exists → append
#             with hdfs_client.write(hdfs_file, append=True, encoding="utf-8") as writer:
#                 writer.write(json.dumps(event) + "\n")

#     except HdfsError as e:
#         print(f"⚠ HDFS write error: {e}")
#         time.sleep(5)

#     print(f"✔ Saved event to {hdfs_file}")
