#!/bin/bash

# ./start-azure-sql-anal.sh
# Start after core services

# SQL Analytics stack

# duckdb
# trino-coordinator

set -e

COMPOSE_FILE="docker-compose.azure.yml"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

log "📊 Starting SQL Analytics stack: Trino & DuckDB..."

# Start Trino and DuckDB
log "🚀 Starting Trino and DuckDB services..."

# More robust port checking with retries
PORT_CHECK_RETRIES=3
PORT_CHECK_DELAY=2
PORT_FREE=true

for ((i=1; i<=PORT_CHECK_RETRIES; i++)); do
    if ss -tuln | grep -q ':8090\b'; then
        log "⚠️ Port 8090 appears in use (attempt $i/$PORT_CHECK_RETRIES)"
        PORT_FREE=false
        sleep $PORT_CHECK_DELAY
    else
        PORT_FREE=true
        break
    fi
done

if ! $PORT_FREE; then
    log "❌ Port 8090 is in use according to multiple checks. Details:"
    ss -tulnp | grep ':8090\b' || true
    docker ps --format '{{.ID}}\t{{.Names}}\t{{.Ports}}' | grep '8090' || true
    log "Trying to identify and kill the conflicting process..."
    sudo lsof -i :8090 || true
    sudo kill -9 $(sudo lsof -t -i :8090) 2>/dev/null || true
    sleep 2
fi

# Final verification
if ss -tuln | grep -q ':8090\b'; then
    log "❌ Could not free port 8090 after cleanup attempts. Aborting."
    exit 1
fi

log "✅ Port 8090 confirmed available after checks"

# Start DuckDB and Trino
docker compose -f "$COMPOSE_FILE" up -d duckdb trino-coordinator

log "✅ SQL analytics services are running."
