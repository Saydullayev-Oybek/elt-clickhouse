import os
import pandas as pd
from utils import download_object, get_date_range, client_connect

def download_price_type():

    url = "https://smartup.online/b/anor/mxs/mkr/price_type$export"  # To‘g‘ri endpointni shu yerga yozing
    
    begin_date, end_date = get_date_range()

    obj_list = download_object(url, 'price_type', begin_date, end_date)

    if not obj_list:
        print("No price type data found.")
        df = pd.DataFrame()
        df.to_parquet("/opt/airflow/dags/data/tmp/price_type.parquet", index=False)
    else:
        df = pd.json_normalize(obj_list)

        if os.path.exists("/opt/airflow/dags/data/sm_f_old/price_type.csv"):
            price_type_old = pd.read_csv("/opt/airflow/dags/data/sm_f_old/price_type.csv")
            merged_price_type = pd.merge(df, price_type_old, on='name', how='left', indicator=True)
            merged_price_type = merged_price_type[merged_price_type['_merge'] == 'left_only'].drop(columns=['_merge'])
            merged_price_type.to_parquet("/opt/airflow/dags/data/tmp/price_type.parquet", index=False)
            df.to_csv("/opt/airflow/dags/data/sm_f_old/price_type.csv", index=False)
        else:
            df.to_parquet("/opt/airflow/dags/data/tmp/price_type.parquet", index=False)
            df.to_csv("/opt/airflow/dags/data/sm_f_old/price_type.csv", index=False)

## Load data into ClickHouse
def load_price_type_clickhouse():

    # ClickHouse server connection
    client = client_connect()

    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.price_type (
        name String,
        code Nullable(String),
        currency_code Nullable(String),
        short_name Nullable(String),
        with_card Nullable(String),
        state Nullable(String),
        price_type_kind Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY name;
    ''')
    
    # ✅ Read from saved files
    if os.path.exists("/opt/airflow/dags/data/tmp/price_type.parquet"):
        price_type = pd.read_parquet("/opt/airflow/dags/data/tmp/price_type.parquet").astype(str)
        if not price_type.empty:
            client.insert_df('raw_proxima.price_type', price_type)

