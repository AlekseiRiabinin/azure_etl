#!/usr/bin/env python3
import duckdb
import os

# Connect to DuckDB
conn = duckdb.connect('/duckdb/main.db')

# Configure S3 access
conn.execute("""
    SET s3_region='us-east-1';
    SET s3_endpoint='minio:9002';
    SET s3_access_key_id='minioadmin';
    SET s3_secret_access_key='minioadmin';
    SET s3_url_style='path';
    SET s3_use_ssl=false;
""")

print("✅ DuckDB configured with MinIO/S3 access")
print("✅ Ready to query: s3a://default/warehouse/")
