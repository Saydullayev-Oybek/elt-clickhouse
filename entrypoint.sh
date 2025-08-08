#!/bin/bash

# STEP 1: Initialize DB
airflow db migrate

# STEP 2: Create admin user (agar yo‘q bo‘lsa)
airflow users create \
    --username admin \
    --firstname Oybek \
    --lastname Saydullayev \
    --role Admin \
    --email admin@example.com \
    --password admin \
    || echo "User already exists"

# STEP 3: Launch webserver
exec airflow webserver
