#!/bin/bash
set -e

echo "Migrating Airflow DB..."
airflow db migrate

# STEP 2: Create admin user (agar yo‘q bo‘lsa)
airflow users create \
    --username admin \
    --firstname Oybek \
    --lastname Saydullayev \
    --role Admin \
    --email admin@example.com \
    --password admin || true

# Start scheduler in background
airflow scheduler &

# Start webserver in foreground
exec airflow webserver