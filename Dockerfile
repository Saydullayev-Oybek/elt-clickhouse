FROM apache/airflow:2.9.0-python3.10

USER root
# (Optional) Install any extra system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    nano curl && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir clickhouse-connect pandas requests pyarrow openpyxl psycopg2-binary django-crispy-forms 

# Copy DAGs and entrypoint script
COPY ./dags /opt/airflow/dags
COPY entrypoint.sh /entrypoint.sh

USER root
RUN chmod +x /entrypoint.sh
USER airflow