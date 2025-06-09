# scripts/kafka_producer.py
from kafka import KafkaProducer
import json
from smart_meter_data import generate_meter_data
import time
import random

def produce_to_kafka():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    while True:
        meter_data = generate_meter_data(1)[0]  # Generate single record
        producer.send('smart_meter_data', meter_data)
        print(f"Produced: {meter_data}")
        time.sleep(random.uniform(0.1, 1.0))  # Random delay between 0.1-1 second

if __name__ == "__main__":
    produce_to_kafka()
