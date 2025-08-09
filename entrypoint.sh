#!/bin/bash
set -e

# If Railway provides PORT, use it for Airflow webserver
export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=${PORT:-8080}
export AIRFLOW__WEBSERVER__WEB_SERVER_HOST=0.0.0.0

# Make sure database is up-to-date
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