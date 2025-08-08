FROM apache/airflow:2.9.0-python3.10

USER root

RUN apt-get update && apt-get install -y supervisor

USER airflow

RUN pip install --no-cache-dir clickhouse-connect pandas requests django-crispy-forms pyarrow openpyxl

COPY ./dags /opt/airflow/dags
COPY ./supervisord.conf /etc/supervisord.conf

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisord.conf"]
