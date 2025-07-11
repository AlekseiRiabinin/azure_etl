#!/bin/bash

# ./start-azure-spark-etl.sh
# Start after core services

# Spark ETL stack

# spark-master
# spark-worker
# jupyterlab

set -e

COMPOSE_FILE="docker-compose.azure.yml"
JUPYTER_URL="http://localhost:8888/"
MAX_RETRIES=30
RETRY_INTERVAL=5

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

check_jupyter_ready() {
    local retries=0
    log "⏳ Waiting for JupyterLab to be ready at $JUPYTER_URL..."
    
    until curl -s -f -o /dev/null "$JUPYTER_URL" || [ $retries -eq $MAX_RETRIES ]; do
        retries=$((retries+1))
        sleep $RETRY_INTERVAL
        log "⌛ Attempt $retries/$MAX_RETRIES - JupyterLab not yet available..."
    done

    if [ $retries -eq $MAX_RETRIES ]; then
        log "❌ Error: JupyterLab did not become available after $MAX_RETRIES attempts"
        return 1
    fi
    
    log "✅ JupyterLab is now available at $JUPYTER_URL"
    return 0
}

log "⚙️ Starting Spark ETL stack: Spark, Hive Metastore, JupyterLab..."

docker compose -f "$COMPOSE_FILE" up -d \
  spark-master \
  spark-worker \
  jupyterlab

log "✅ Spark ETL services are running."

# Check JupyterLab availability
if ! check_jupyter_ready; then
    log "⚠️ Proceeding without JupyterLab availability confirmation"
    log "💡 Try checking JupyterLab logs with: docker logs jupyterlab"
fi

log "🚀 Spark ETL stack initialization complete"
