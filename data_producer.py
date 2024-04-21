from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# Kafka broker address
bootstrap_servers = 'localhost:9092'

# Kafka topic to produce data to
topic = 'topic1'

#KafkaProducer instance
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Generate and push data to Kafka
while True:
    Time = datetime.now().strftime("%I:%M:%S %p")
    day = datetime.now().day
    day_of_week = datetime.now().strftime("%A")
    car_count = random.randint(6, 180)
    bike_count = random.randint(0, 70)
    bus_count = random.randint(0, 50)
    truck_count = random.randint(0, 40)
    total = car_count + bike_count + bus_count + truck_count
    
    traffic_data = {
        "time": Time,
        "Day": day,
        "Day_of_the_week": day_of_week,
        "CarCount": car_count,
        "BikeCount": bike_count,
        "BusCount": bus_count,
        "TruckCount": truck_count,
        "Total": total,
    }
    try:
        # Push data to Kafka topic
        producer.send(topic, value=traffic_data)
        print("Data Sent:", traffic_data)
    except Exception as e:
        print(f"Error occurred while sending data to Kafka: {e}")
        
    print("Data Sent:", traffic_data)
    
    time.sleep(30)  # 30 seconds delay


