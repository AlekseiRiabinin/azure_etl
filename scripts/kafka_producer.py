from kafka import KafkaProducer
import json
from smart_meter_data import generate_meter_data
import time
import random
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('kafka')
logger.setLevel(logging.DEBUG)

def produce_to_kafka():
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka-broker:9092', 'kafka-broker:9095'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(3, 8, 0),  # Match your Kafka version
            security_protocol='PLAINTEXT',
            request_timeout_ms=10000,
            retries=5
        )
        
        while True:
            try:
                meter_data = generate_meter_data(1)[0]
                future = producer.send('smart_meter_data', meter_data)
                # Wait for acknowledgment
                metadata = future.get(timeout=10)
                print(f"Produced to {metadata.topic}[{metadata.partition}] at offset {metadata.offset}")
                time.sleep(random.uniform(0.1, 1.0))
            except Exception as e:
                print(f"Error producing message: {str(e)}")
                time.sleep(5)  # Backoff before retry

    except Exception as e:
        print(f"Fatal producer error: {str(e)}")
        raise

if __name__ == "__main__":
    produce_to_kafka()