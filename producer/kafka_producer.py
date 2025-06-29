from kafka import KafkaProducer
from smart_meter_data import generate_meter_data
import json
import time
import random
import logging
import os

# Constants
BOOTSTRAP_SERVERS = ['kafka-1:9092', 'kafka-2:9095']
TOPIC = 'smart_meter_data'
DATA_PATH = "/app/data/smart_meter_data.json"

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger('kafka-producer')
logger.setLevel(logging.DEBUG)

def validate_environment() -> None:
    """Check required files and connections."""
    if not os.path.exists(DATA_PATH):
        logger.error(f"Data file not found at {DATA_PATH}. Verify volume mount!")
        raise FileNotFoundError(f"Missing data file: {DATA_PATH}")

def create_producer() -> None:
    """Create and configure Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(3, 8),
        security_protocol='PLAINTEXT',
        request_timeout_ms=30000,
        retries=5,
        reconnect_backoff_ms=1000,
        metadata_max_age_ms=30000
    )

def produce_to_kafka() -> None:
    """Send data to Kafka-brokers."""
    validate_environment()
    
    try:
        producer = create_producer()
        logger.info(f"Producer initialized, sending to topic: {TOPIC}")

        while True:
            try:
                # Generate and send data
                meter_data = generate_meter_data(1)[0]
                future = producer.send(TOPIC, meter_data)
                
                # Verify delivery
                metadata = future.get(timeout=10)
                logger.debug(
                    f"Produced to {metadata.topic}[{metadata.partition}] "
                    f"at offset {metadata.offset}"
                )
                
                # Throttle production
                time.sleep(random.uniform(0.1, 1.0))
                
            except Exception as e:
                logger.error(f"Production error: {str(e)}", exc_info=True)
                time.sleep(5)  # Backoff before retry

    except Exception as e:
        logger.critical(f"Fatal producer error: {str(e)}", exc_info=True)
        raise
    finally:
        producer.close() if 'producer' in locals() else None

if __name__ == "__main__":
    produce_to_kafka()
