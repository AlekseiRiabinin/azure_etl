#!/bin/bash

set -e  # Exit on error

COMPOSE_FILE="docker-compose.azure.yml"

echo "🛑 Stopping all containers from $COMPOSE_FILE..."
docker compose -f $COMPOSE_FILE down

# echo "🧼 Removing unused Docker resources (volumes/images can be manually pruned if needed)..."
# docker system prune -f

echo -e "\n✅ All services have been stopped and cleaned up."
