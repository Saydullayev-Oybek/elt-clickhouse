#!/bin/bash
set -e

echo "=== Starting Airflow with Postgres backend ==="

# Wait for Postgres to be ready
echo "Waiting for Postgres..."
sleep 5

# Initialize database (safe to run every time)
airflow db upgrade

# STEP 2: Create admin user (agar yo‘q bo‘lsa)
airflow users create \
    --username admin \
    --firstname Oybek \
    --lastname Saydullayev \
    --role Admin \
    --email admin@example.com \
    --password admin || true


# Start both scheduler and webserver via supervisord
cat <<EOF > /etc/supervisord.conf
[supervisord]
nodaemon=true

[program:scheduler]
command=airflow scheduler
autostart=true
autorestart=true
startsecs=5

[program:webserver]
command=airflow webserver --port 8080
autostart=true
autorestart=true
startsecs=5
EOF

exec /usr/bin/supervisord -c /etc/supervisord.conf