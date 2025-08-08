FROM apache/airflow:2.9.0-python3.10

# Faylni root user huquqida copy qilamiz va chmod ham rootda bajariladi
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER airflow
RUN pip install --no-cache-dir clickhouse-connect pandas requests django-crispy-forms pyarrow openpyxl

ENTRYPOINT ["/entrypoint.sh"]
