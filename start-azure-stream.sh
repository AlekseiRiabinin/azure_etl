#!/bin/bash

# Start after core services

# Streaming tools

# kafdrop
# kafka-producer

set -e

COMPOSE_FILE="docker-compose.azure.yml"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

log "📡 Starting Streaming tools: Kafka Producer and Kafdrop..."

# Ensure Kafka topics exist first
echo "🧪 Creating Kafka topics (if missing)..."
docker exec kafka-1 bash -c '
    topics=("smart_meter_data:4:2" "weather_data:2:2")
    for topic in "${topics[@]}"; do
        IFS=":" read -r name partitions replicas <<< "$topic"
        if ! kafka-topics.sh --describe --topic "$name" --bootstrap-server kafka-1:9092 >/dev/null 2>&1; then
            echo "Creating topic: $name"
            kafka-topics.sh --create \
                --topic "$name" \
                --partitions "$partitions" \
                --replication-factor "$replicas" \
                --bootstrap-server kafka-1:9092
        else
            echo "Topic $name already exists."
        fi
    done
'

docker compose -f $COMPOSE_FILE up -d \
  kafdrop \
  kafka-producer

log "✅ Kafka tools (Kafdrop, Producer) are running."
