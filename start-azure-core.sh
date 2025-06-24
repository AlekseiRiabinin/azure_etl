#!/bin/bash

# ./start-azure-core.sh
# Minimal services required for all workflows

# kafka-1
# kafka-2
# postgres
# hive-metastore
# minio

set -e

COMPOSE_FILE="docker-compose.azure.yml"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

log "🔧 Starting CORE services: postgres, minio, kafka broker..."

docker compose -f $COMPOSE_FILE up -d \
  kafka-1 \
  kafka-2 \
  postgres \
  hive-metastore \
  minio

# Step 1: Check Kafka brokers and create topics
log "⏳ Waiting for Kafka brokers to be ready..."
for broker in kafka-1:9092 kafka-2:9095; do
    container=$(echo $broker | cut -d':' -f1)
    port=$(echo $broker | cut -d':' -f2)
    
    MAX_WAIT=120
    WAITED=0
    INTERVAL=5
    
    while ! docker exec $container kafka-broker-api-versions.sh --bootstrap-server $container:$port >/dev/null 2>&1; do
        sleep $INTERVAL
        WAITED=$((WAITED + INTERVAL))
        log "Waiting for $container... ($WAITED/$MAX_WAIT seconds)"
        if [ "$WAITED" -ge "$MAX_WAIT" ]; then
            log "❌ Timed out waiting for $container"
            exit 1
        fi
    done
done
log "✅ Kafka brokers are ready"

log "📊 Setting up Kafka topics..."

# topic:partitions:replicas
docker exec kafka-1 bash -c '
    topics=("smart_meter_data:4:2" "weather_data:2:2")
    
    for topic in "${topics[@]}"; do
        IFS=":" read -r name partitions replicas <<< "$topic"
        
        if ! kafka-topics.sh --describe --topic "$name" --bootstrap-server kafka-1:9092 >/dev/null 2>&1; then
            echo "[$(date "+%Y-%m-%d %H:%M:%S")] Creating topic: $name"
            kafka-topics.sh --create \
                --topic "$name" \
                --partitions "$partitions" \
                --replication-factor "$replicas" \
                --bootstrap-server kafka-1:9092,kafka-2:9095
        else
            echo "[$(date "+%Y-%m-%d %H:%M:%S")] Topic $name exists"
        fi
    done
'
log "✅ Kafka topics ready"

# Step 2: Wait for postgres to be healthy
log "⏳ Waiting for postgres to be ready..."
MAX_WAIT=120
WAITED=0
INTERVAL=5

until [ "$(docker inspect -f '{{.State.Health.Status}}' postgres 2>/dev/null || echo unhealthy)" == "healthy" ]; do
  sleep $INTERVAL
  WAITED=$((WAITED + INTERVAL))
  log "Still waiting for postgres ($WAITED sec elapsed)"
  if [ "$WAITED" -ge "$MAX_WAIT" ]; then
    log "❌ Timed out waiting for postgres."
    exit 1
  fi
done

# Step 3: Initialize Hive Metastore
HIVE_DB_NAME=hive_metastore
HIVE_CONTAINER=hive-metastore

log "🧪 Setting up Hive Metastore..."

# First ensure the database exists
DB_EXISTS=$(docker exec postgres psql -U postgres -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = '$HIVE_DB_NAME';")
if [ "$DB_EXISTS" != "1" ]; then
  log "🛠️ Creating database $HIVE_DB_NAME..."
  docker exec postgres psql -U postgres -d postgres -c "CREATE DATABASE $HIVE_DB_NAME;"
  log "✅ Database $HIVE_DB_NAME created."
fi

# Verify schema (entrypoint already initialized it)
log "🔍 Verifying Hive Metastore schema..."
if docker exec $HIVE_CONTAINER schematool -dbType postgres -info; then
  log "✅ Hive Metastore schema verified successfully"
else
  log "❌ Hive Metastore schema verification failed"
  docker logs $HIVE_CONTAINER
  exit 1
fi

# Step 4: Create 'default' bucket in MinIO
log "📦 Ensuring 'default' bucket exists in MinIO..."

set +e  # Temporarily disable exit-on-error
docker run --rm --network azure_etl_kafka-net minio/mc mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1
BUCKET_EXISTS=$(docker run --rm --network azure_etl_kafka-net minio/mc mc ls local 2>/dev/null | grep -c 'default')
set -e  # Re-enable strict mode

if [ "$BUCKET_EXISTS" -eq 0 ]; then
  log "🪣  Creating MinIO bucket: 'default'"
  docker run --rm --network azure_etl_kafka-net minio/mc \
    mc alias set local http://minio:9000 minioadmin minioadmin && \
    docker run --rm --network azure_etl_kafka-net minio/mc \
      mc mb local/default
else
  log "✅ MinIO bucket 'default' already exists."
fi

log "✅ Core services are running."
