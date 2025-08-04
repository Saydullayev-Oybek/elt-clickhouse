import os
import pandas as pd
import clickhouse_connect


## Download data from API
def download_warehouse():
        
        if os.path.exists("/opt/airflow/dags/data/sm_f/warehouse.xlsx"):

            warehouse = pd.read_excel("/opt/airflow/dags/data/sm_f/warehouse.xlsx").rename(
                columns={
                    'ИД склада': 'warehouse_id',
                    'Название': 'name',
                    'Тип склада': 'type',
                    'Материально ответственное лицо': 'financial_responsible',
                    'Статус': 'status',
                    'Регион': 'region',
                    'GPS координаты': 'latlng',
                    'Адрес': 'address',
                    'Влажность помещения': 'humidity',
                    'Высота': 'height',
                    'Дата последнего изменения': 'date_modification',
                    'Дата создания': 'date_creation',
                    'Диаметр': 'diameter',
                    'Длина': 'length',
                    'Изменил': 'changed_by',
                    'Код': 'code',
                    'Объем': 'volume',
                    'Ориентир': 'landmark',
                    'Параметры объема': 'volume_parameters',
                    'Порядковый номер': 'order_no',
                    'Примечание': 'note',
                    'Создал': 'created_by',
                    'Температурный режим': 'temperature_mode',
                    'Ширина': 'width'
                    }
            )

            if os.path.exists("/opt/airflow/dags/data/sm_f_old/warehouse.csv"):
                warehouse_old = pd.read_csv("/opt/airflow/dags/data/sm_f_old/warehouse.csv")
                merged_warehouse = pd.merge(warehouse, warehouse_old, on='warehouse_id', how='left', indicator=True)
                merged_warehouse = merged_warehouse[merged_warehouse['_merge'] == 'left_only'].drop(columns=['_merge'])
                warehouse.to_csv("/opt/airflow/dags/data/sm_f_old/warehouse.csv", index=False)
                merged_warehouse.to_csv("/opt/airflow/dags/data/sm_f/warehouse_to_load.csv", index=False)
            else:
                warehouse.to_csv("/opt/airflow/dags/data/sm_f_old/warehouse.csv", index=False)
                warehouse.to_csv("/opt/airflow/dags/data/sm_f/warehouse_to_load.csv", index=False)


## Load data into ClickHouse
def load_warehouse_clickhouse():

    # ClickHouse server connection
    client = clickhouse_connect.get_client(
        host='kz1-a-0qptbicmd7nfc07f.mdb.yandexcloud.kz', 
        port=8443,
        username='admin',
        password='admin123',
        secure=True,
        verify=False
    )

    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.warehouse(
            warehouse_id String,
            name Nullable(String),
            type Nullable(String),
            financial_responsible Nullable(String), 
            status Nullable(String),
            region Nullable(String),
            latlng Nullable(String),
            address Nullable(String),
            humidity Nullable(String),
            height Nullable(String),
            date_modification Nullable(String),
            date_creation Nullable(String),
            diameter Nullable(String),
            length Nullable(String),
            changed_by Nullable(String),
            code Nullable(String),
            volume Nullable(String),
            landmark Nullable(String),
            volume_parameters Nullable(String),
            order_no Nullable(String),
            note Nullable(String),
            created_by Nullable(String),
            temperature_mode Nullable(String),
            width Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY warehouse_id;
    ''')    

    
    # ✅ Read from saved files
    if os.path.exists("/opt/airflow/dags/data/sm_f/warehouse_to_load.csv"):
        warehouse = pd.read_csv("/opt/airflow/dags/data/sm_f/warehouse_to_load.csv").astype(str)
        if not warehouse.empty:
            client.insert_df('raw_proxima.warehouse', warehouse)


