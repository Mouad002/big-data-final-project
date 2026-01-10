import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = "kafka:29092"
TOPIC = "traffic-events"

while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        break
    except NoBrokersAvailable:
        print("Kafka not ready, retrying in 5s...")
        time.sleep(5)

SENSORS = ["S1", "S2", "S3", "S4"]
ROADS = [
    {"road_id": "R1", "road_type": "avenue", "zone": "Downtown"},
    {"road_id": "R2", "road_type": "street", "zone": "Residential"},
    {"road_id": "R3", "road_type": "highway", "zone": "North"},
]

def is_rush_hour(hour):
    return 7 <= hour <= 9 or 17 <= hour <= 19

print("🚦 Traffic generator started...")

while True:
    now = datetime.now()
    rush = is_rush_hour(now.hour)

    road = random.choice(ROADS)

    vehicle_count = random.randint(30, 60) if rush else random.randint(5, 25)
    average_speed = random.uniform(15, 35) if rush else random.uniform(40, 80)
    occupancy_rate = min(100, int(vehicle_count * 1.5))

    event = {
        "sensor_id": random.choice(SENSORS),
        "road_id": road["road_id"],
        "road_type": road["road_type"],
        "zone": road["zone"],
        "vehicle_count": vehicle_count,
        "average_speed": round(average_speed, 2),
        "occupancy_rate": occupancy_rate,
        "event_time": now.isoformat()
    }

    producer.send(TOPIC, event)
    print(event)

    time.sleep(1)
