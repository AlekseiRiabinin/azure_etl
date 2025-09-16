#!/bin/bash

set -e

echo "Initializing Hive schema with PostgreSQL support..."

schematool -dbType postgres -initSchema --verbose || echo "Schema may already be initialized."

exec hive --service metastore
