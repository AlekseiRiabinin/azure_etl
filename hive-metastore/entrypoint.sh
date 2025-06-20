#!/bin/bash
set -e

echo "Waiting for PostgreSQL at ${DB_HOST}:${DB_PORT}..."

# More robust PostgreSQL connection check using psql
until pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER"; do
  sleep 2
done
echo "PostgreSQL is available and accepting connections."

# Create marker directory if it doesn't exist
mkdir -p /var/hive

# Improved schema initialization logic
INIT_MARKER="/var/hive/metastore_initialized"
if [ ! -f "$INIT_MARKER" ]; then
  echo "Checking Hive Metastore schema status..."
  if schematool -dbType "$DB_TYPE" -info; then
    echo "Hive Metastore schema already exists in database."
  else
    echo "Initializing Hive Metastore schema..."
    if schematool -dbType "$DB_TYPE" -initSchema; then
      echo "Schema initialization completed successfully."
    else
      echo "[WARNING] Schema initialization failed (possibly already initialized). Continuing..."
    fi
  fi
  touch "$INIT_MARKER"
else
  echo "Hive Metastore schema initialization already attempted (marker file exists)."
fi

# Verify schema is operational before starting
echo "Verifying schema connection..."
MAX_RETRIES=3
RETRY_DELAY=5
for ((i=1; i<=$MAX_RETRIES; i++)); do
  if schematool -dbType "$DB_TYPE" -info; then
    echo "Schema verification successful."
    break
  else
    echo "[Attempt $i/$MAX_RETRIES] Schema verification failed. Retrying..."
    sleep $RETRY_DELAY
  fi
  
  if [ $i -eq $MAX_RETRIES ]; then
    echo "[ERROR] Failed to verify schema after $MAX_RETRIES attempts. Aborting."
    exit 1
  fi
done

echo "Starting Hive Metastore..."
exec hive --service metastore "$@"
