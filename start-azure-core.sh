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

# Step 0: Start MinIO FIRST and create bucket BEFORE other services
log "🔧 Starting MinIO first..."
docker compose -f $COMPOSE_FILE up -d minio

# Give MinIO some time to start
log "⏳ Giving MinIO time to initialize..."
sleep 10

# Step 1: Create MinIO bucket with robust error handling
log "📦 Creating MinIO bucket: 'default'..."
set +e

# Try multiple times to create the bucket
MAX_BUCKET_ATTEMPTS=5
BUCKET_ATTEMPTS=0
BUCKET_CREATED=false

while [ $BUCKET_ATTEMPTS -lt $MAX_BUCKET_ATTEMPTS ] && [ "$BUCKET_CREATED" = false ]; do
    BUCKET_ATTEMPTS=$((BUCKET_ATTEMPTS + 1))
    
    # Set alias and create bucket
    if docker run --rm --network azure_etl_kafka-net minio/mc \
        mc alias set local http://minio:9002 minioadmin minioadmin >/dev/null 2>&1; then
        
        if docker run --rm --network azure_etl_kafka-net minio/mc \
            mc mb local/default >/dev/null 2>&1; then
            log "✅ MinIO bucket 'default' created successfully (attempt $BUCKET_ATTEMPTS)"
            BUCKET_CREATED=true
        else
            # Check if bucket already exists
            if docker run --rm --network azure_etl_kafka-net minio/mc \
                mc ls local/default >/dev/null 2>&1; then
                log "✅ MinIO bucket 'default' already exists (attempt $BUCKET_ATTEMPTS)"
                BUCKET_CREATED=true
            else
                log "⚠️  Bucket creation failed on attempt $BUCKET_ATTEMPTS, retrying..."
                sleep 10
            fi
        fi
    else
        log "⚠️  MinIO not ready for alias setup (attempt $BUCKET_ATTEMPTS), retrying..."
        sleep 10
    fi
done

set -e

if [ "$BUCKET_CREATED" = false ]; then
    log "⚠️  Could not create or verify 'default' bucket, but continuing anyway..."
    log "⚠️  You may need to create it manually: mc mb local/default"
fi

# Step 2: Now start the other services AFTER bucket is created
log "🚀 Starting other core services: postgres, kafka brokers, hive-metastore..."
docker compose -f $COMPOSE_FILE up -d \
  kafka-1 \
  kafka-2 \
  postgres \
  hive-metastore

# Step 3: Check Kafka brokers and create topics
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

# Step 4: Wait for postgres to be healthy
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

# Step 5: Initialize Hive Metastore
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
MAX_RETRIES=5
RETRY_DELAY=10
for ((i=1; i<=$MAX_RETRIES; i++)); do
  if docker exec $HIVE_CONTAINER schematool -dbType postgres -info; then
    log "✅ Hive Metastore schema verified successfully"
    break
  else
    if [ $i -eq $MAX_RETRIES ]; then
      log "❌ Hive Metastore schema verification failed after $MAX_RETRIES attempts"
      docker logs $HIVE_CONTAINER
      exit 1
    fi
    log "⚠️  Schema verification attempt $i/$MAX_RETRIES failed, retrying..."
    sleep $RETRY_DELAY
  fi
done

log "✅ Core services are running successfully!"
log "📊 MinIO: http://localhost:9001 (minioadmin/minioadmin)"
log "📊 Hive Metastore: Ready on port 9083"
log "📊 PostgreSQL: Ready on port 5434"
log "📊 Kafka: Brokers ready on 9092, 9095"
