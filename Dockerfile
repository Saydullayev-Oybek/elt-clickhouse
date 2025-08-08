FROM apache/airflow:2.9.0-python3.10

USER airflow
RUN pip install --no-cache-dir clickhouse-connect pandas requests django-crispy-forms pyarrow openpyxl

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
