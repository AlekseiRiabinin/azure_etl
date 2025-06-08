#!/bin/bash

set -e  # Exit on any error

COMPOSE_FILE="docker-compose.azure.yml"

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

# Step 2: Wait for postgres to be healthy
echo "⏳ Waiting for postgres to be ready..."
MAX_WAIT=120
WAITED=0
INTERVAL=5

until [ "$(docker inspect -f '{{.State.Health.Status}}' postgres 2>/dev/null || echo unhealthy)" == "healthy" ]; do
  sleep $INTERVAL
  WAITED=$((WAITED + INTERVAL))
  echo "  ...still waiting for postgres ($WAITED sec elapsed)"
  if [ "$WAITED" -ge "$MAX_WAIT" ]; then
    echo "❌ Timed out waiting for postgres."
    exit 1
  fi
done

# Step 3: Create 'default' bucket in MinIO
echo "📦 Ensuring 'default' bucket exists in MinIO..."

set +e  # Temporarily disable exit-on-error
docker run --rm --network azure_etl_kafka-net minio/mc mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1
BUCKET_EXISTS=$(docker run --rm --network azure_etl_kafka-net minio/mc mc ls local 2>/dev/null | grep -c 'default')
set -e  # Re-enable strict mode

if [ "$BUCKET_EXISTS" -eq 0 ]; then
  echo "🪣 Creating MinIO bucket: 'default'"
  docker run --rm --network azure_etl_kafka-net minio/mc \
    mc alias set local http://minio:9000 minioadmin minioadmin && \
    docker run --rm --network azure_etl_kafka-net minio/mc \
      mc mb local/default
else
  echo "✅ MinIO bucket 'default' already exists."
fi

# Step 4: Initialize Airflow database (runs once with cleanup)
echo "🔍 Checking if Airflow DB is already initialized..."

INIT_CHECK=$(docker exec postgres psql -U postgres -d postgres -tAc "SELECT 1 FROM information_schema.tables WHERE table_name='dag';")

if [ "$INIT_CHECK" == "1" ]; then
  echo "✅ Airflow metadata DB already initialized. Skipping airflow-init."
else
  echo "🔄 Initializing Airflow metadata DB..."
  docker compose -f $COMPOSE_FILE up -d airflow-init
  docker compose -f $COMPOSE_FILE logs -f airflow-init
fi

# Step 5: Start Airflow webserver and scheduler
echo "🚀 Starting Airflow services..."
docker compose -f $COMPOSE_FILE up -d airflow-webserver airflow-scheduler

# Step 6: Wait until Airflow Webserver is responsive
echo "⏳ Waiting for Airflow webserver to respond..."
MAX_WAIT=120
WAITED=0

until docker exec airflow-webserver curl -s localhost:8080 > /dev/null 2>&1; do
  sleep $INTERVAL
  WAITED=$((WAITED + INTERVAL))
  echo "  ...still waiting for Airflow webserver ($WAITED sec elapsed)"
  if [ "$WAITED" -ge "$MAX_WAIT" ]; then
    echo "❌ Timed out waiting for Airflow webserver."
    exit 1
  fi
done

# Step 7: Verify admin user creation (in case init step skipped it)
echo "👤 Verifying Airflow admin user..."
if ! docker exec airflow-webserver airflow users list | grep -q admin; then
  echo "👤 Creating Airflow admin user..."
  docker exec airflow-webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
else
  echo "✅ Admin user already exists."
fi

# Step 8: Start Trino and DuckDB
echo "🚀 Starting Trino and DuckDB services..."
docker compose -f $COMPOSE_FILE up -d duckdb trino-coordinator

# Step 9: Output access info
echo -e "\n✅ All services are up and running!"
echo -e "➡️  Access Airflow UI:     http://localhost:8083"
echo -e "➡️  Access Kafka UI:       http://localhost:9002"
echo -e "➡️  Access MinIO Console:  http://localhost:9001"
echo -e "➡️  Access JupyterLab:     http://localhost:8888"
echo -e "➡️  Access Trino UI:       http://localhost:8088"

echo -e "\n📌 Airflow Postgres Connection (if needed):"
echo "  - Conn ID: azure_postgres"
echo "  - Type: Postgres"
echo "  - Host: postgres"
echo "  - Port: 5432"
echo "  - Username: postgres"
echo "  - Password: postgres"
echo "  - Schema: postgres"
