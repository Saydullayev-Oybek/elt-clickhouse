FROM apache/airflow:2.9.0-python3.10

# Switch to root to copy files
USER root

# Install OS deps (optional)
RUN apt-get update && apt-get install -y --no-install-recommends \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy requirements if you have them
USER airflow
RUN pip install --no-cache-dir clickhouse-connect pandas requests pyarrow openpyxl django-crispy-forms

# Copy DAGs
COPY ./dags /opt/airflow/dags

# Copy entrypoint script and make executable
COPY --chmod=755 ./entrypoint.sh /entrypoint.sh

# Switch back to airflow user
USER airflow

# Airflow ENV vars
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor

# IMPORTANT: use your Railway Postgres connection string
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:KPFqRIlSoTiFOVisEyjCkXeUcnBIkImj@centerbeam.proxy.rlwy.net:30385/railway

EXPOSE 8080

ENTRYPOINT ["/entrypoint.sh"]
