FROM python:3.9-slim

WORKDIR /app

# Copy only the producer scripts (no data)
COPY producer/kafka_producer.py .
COPY producer/smart_meter_data.py .

# Install dependencies
RUN pip install --no-cache-dir kafka-python==2.2.11

CMD ["python", "kafka_producer.py"]