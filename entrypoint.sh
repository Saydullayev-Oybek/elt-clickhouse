#!/bin/bash
set -e

airflow db init

# STEP 2: Create admin user (agar yo‘q bo‘lsa)
airflow users create \
    --username admin \
    --firstname Oybek \
    --lastname Saydullayev \
    --role Admin \
    --email admin@example.com \
    --password admin \
    || echo "User already exists"

# Start scheduler in background
airflow scheduler &

# Start webserver in foreground
exec airflow webserver