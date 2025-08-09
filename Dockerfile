FROM apache/airflow:2.9.0-python3.10

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy DAGs
COPY ./dags /opt/airflow/dags

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Default Airflow env vars
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql://postgres:KPFqRIlSoTiFOVisEyjCkXeUcnBIkImj@centerbeam.proxy.rlwy.net:30385/railway

EXPOSE 8080

ENTRYPOINT ["/entrypoint.sh"]
