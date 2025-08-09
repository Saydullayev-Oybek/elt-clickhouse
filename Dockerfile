FROM apache/airflow:2.9.0-python3.10

USER root
# Install any OS packages you might need
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir clickhouse-connect pandas requests pyarrow openpyxl

# Copy DAGs and entrypoint script
COPY ./dags /opt/airflow/dags
COPY entrypoint.sh /entrypoint.sh

# Make entrypoint executable
USER root
RUN chmod +x /entrypoint.sh
USER airflow

# Environment variables for Airflow
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor

# Railway will run this script
ENTRYPOINT ["/entrypoint.sh"]
