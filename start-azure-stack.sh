#!/bin/bash

set -e  # Exit on any error

COMPOSE_FILE="docker-compose.azure.yml"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Step 1: Start core infrastructure services
echo "🚀 Starting base infrastructure services..."
docker compose -f $COMPOSE_FILE up -d \
  postgres \
  kafka-1 \
  kafka-2 \
  spark-master \
  spark-worker \
  minio \
  jupyterlab \
  kafdrop

# Step 2: Check Kafka brokers and create topics
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
log "✅ Both Kafka brokers are ready"

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

# Step 3: Wait for postgres to be healthy
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

# Step 4: Create 'default' bucket in MinIO
log "📦 Ensuring 'default' bucket exists in MinIO..."

set +e  # Temporarily disable exit-on-error
docker run --rm --network azure_etl_kafka-net minio/mc mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1
BUCKET_EXISTS=$(docker run --rm --network azure_etl_kafka-net minio/mc mc ls local 2>/dev/null | grep -c 'default')
set -e  # Re-enable strict mode

if [ "$BUCKET_EXISTS" -eq 0 ]; then
  log "🪣 Creating MinIO bucket: 'default'"
  docker run --rm --network azure_etl_kafka-net minio/mc \
    mc alias set local http://minio:9000 minioadmin minioadmin && \
    docker run --rm --network azure_etl_kafka-net minio/mc \
      mc mb local/default
else
  log "✅ MinIO bucket 'default' already exists."
fi

# Step 5: Initialize Airflow database (runs once with cleanup)
log "🔍 Checking if Airflow DB is already initialized..."

INIT_CHECK=$(docker exec postgres psql -U postgres -d postgres -tAc "SELECT 1 FROM information_schema.tables WHERE table_name='dag';")

if [ "$INIT_CHECK" == "1" ]; then
  log "✅ Airflow metadata DB already initialized. Skipping airflow-init."
else
  log "🔄 Initializing Airflow metadata DB..."
  docker compose -f $COMPOSE_FILE up -d airflow-init
  docker compose -f $COMPOSE_FILE logs -f airflow-init
fi

# Step 6: Start Airflow webserver and scheduler
log "🚀 Starting Airflow services..."
docker compose -f $COMPOSE_FILE up -d airflow-webserver airflow-scheduler

# Step 7: Wait until Airflow Webserver is responsive
log "⏳ Waiting for Airflow webserver to respond..."
MAX_WAIT=120
WAITED=0

until docker exec airflow-webserver curl -s localhost:8080 > /dev/null 2>&1; do
  sleep $INTERVAL
  WAITED=$((WAITED + INTERVAL))
  log "Still waiting for Airflow webserver ($WAITED sec elapsed)"
  if [ "$WAITED" -ge "$MAX_WAIT" ]; then
    log "❌ Timed out waiting for Airflow webserver."
    exit 1
  fi
done

# Step 8: Verify admin user creation (in case init step skipped it)
log "👤 Verifying Airflow admin user..."
if ! docker exec airflow-webserver airflow users list | grep -q admin; then
  log "👤 Creating Airflow admin user..."
  docker exec airflow-webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
else
  log "✅ Admin user already exists."
fi

# Step 9: Start Trino and DuckDB
log "🚀 Starting Trino and DuckDB services..."
docker compose -f $COMPOSE_FILE up -d duckdb trino-coordinator

# Step 10: Output access info
log "✅ All services are up and running!"
log "➡️  Access Airflow UI:     http://localhost:8083"
log "➡️  Access Kafka UI:       http://localhost:9002"
log "➡️  Access MinIO Console:  http://localhost:9001"
log "➡️  Access JupyterLab:     http://localhost:8888"
log "➡️  Access Trino UI:       http://localhost:8088"

log "📌 Airflow Postgres Connection (if needed):"
log "  - Conn ID: azure_postgres"
log "  - Type: Postgres"
log "  - Host: postgres"
log "  - Port: 5432"
log "  - Username: postgres"
log "  - Password: postgres"
log "  - Schema: postgres"
