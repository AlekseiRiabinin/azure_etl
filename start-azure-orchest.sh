#!/bin/bash

# Start after core services

# Orchestration (Airflow)

# airflow-init
# airflow-webserver
# airflow-scheduler

set -e

COMPOSE_FILE="docker-compose.azure.yml"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

log "📦 Starting Orchestration (Airflow)..."

# Step 1: Initialize Airflow database (runs once with cleanup)
log "🔍 Checking if Airflow DB is already initialized..."

INIT_CHECK=$(docker exec postgres psql -U postgres -d postgres -tAc "SELECT 1 FROM information_schema.tables WHERE table_name='dag';")

if [ "$INIT_CHECK" == "1" ]; then
  log "✅ Airflow metadata DB already initialized. Skipping airflow-init."
else
  log "🔄 Initializing Airflow metadata DB..."
  docker compose -f $COMPOSE_FILE up -d airflow-init
  docker compose -f $COMPOSE_FILE logs -f airflow-init
fi

# Step 2: Start Airflow webserver and scheduler
log "🚀 Starting Airflow services..."
docker compose -f $COMPOSE_FILE up -d airflow-webserver airflow-scheduler

# Step 3: Wait until Airflow Webserver is responsive
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

# Step 4: Verify admin user creation (in case init step skipped it)
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

log "✅ Airflow orchestration is running."
