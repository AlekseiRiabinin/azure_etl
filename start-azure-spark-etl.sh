#!/bin/bash

# ./start-azure-spark-etl.sh
# Start after core services

# Spark ETL stack

# spark-master
# spark-worker
# jupyterlab

set -e

COMPOSE_FILE="docker-compose.azure.yml"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

log "⚙️ Starting Spark ETL stack: Spark, Hive Metastore, JupyterLab..."

docker compose -f docker-compose.azure.yml up -d \
  spark-master \
  spark-worker \
  jupyterlab

log "✅ Spark ETL services are running."
