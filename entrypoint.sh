#!/bin/bash
set -e

echo ">>> Initializing Airflow DB..."
airflow db reset -y
airflow db migrate

echo ">>> Checking for existing admin user..."
if ! airflow users list | grep -q "admin"; then
    echo ">>> Creating default admin user..."
    airflow users create \
        --username admin \
        --firstname Oybek \
        --lastname Saydullayev \
        --role Admin \
        --email admin@example.com \
        --password admin || true
else
    echo ">>> Admin user already exists."
fi

echo ">>> Starting Airflow scheduler..."
airflow scheduler &

echo ">>> Starting Airflow webserver..."
exec airflow webserver