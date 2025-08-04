import os
import pandas as pd
from utils import download_object, get_date_range, save_sync_date, client_connect


def download_product_group():
        
    url = "https://smartup.online/b/anor/mxsx/mr/product_group$export"

    begin_date, end_date = get_date_range()

    obj_list = download_object(url, 'product_group', begin_date, end_date)

    if not obj_list:
        print("No product group data found.")
        product_group = pd.DataFrame()
        product_group_type = pd.DataFrame()
        product_group.to_parquet("/opt/airflow/dags/data/tmp/product_group.parquet", index=False)
        product_group_type.to_parquet("/opt/airflow/dags/data/tmp/product_group_type.parquet", index=False)
        save_sync_date(end_date)
    else:
        product_group = pd.json_normalize(obj_list).drop(columns=['product_group_types'])
        print(product_group.columns)

        product_group_type = []
        for i in obj_list:
            # print(i['product_group_types'])
            if i['product_group_types']:
                for j in i['product_group_types']:
                    j['product_group_id'] = i['product_group_id']
                    product_group_type.append(j)

        product_group_type = pd.json_normalize(product_group_type)
        # Save data to parquet files
        product_group.to_parquet("/opt/airflow/dags/data/tmp/product_group.parquet", index=False)
        product_group_type.to_parquet("/opt/airflow/dags/data/tmp/product_group_type.parquet", index=False)
        save_sync_date(end_date)


## Load data into ClickHouse
def load_product_group_clickhouse():

    # ClickHouse server connection
    client = client_connect()

    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.product_group (
        product_group_id String,
        code Nullable(String),
        name Nullable(String),
        product_kind Nullable(String),
        state Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY product_group_id;
    ''')
    
    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.product_group_type (
        product_type_id String,
        product_group_id Nullable(String),
        code Nullable(String),
        name Nullable(String),
        state Nullable(String),
        order_no Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY product_type_id;
    ''')

    # ✅ Read from saved files
    if os.path.exists("/opt/airflow/dags/data/tmp/product_group.parquet"):
        product_group = pd.read_parquet("/opt/airflow/dags/data/tmp/product_group.parquet")
        if not product_group.empty:
            client.insert_df('raw_proxima.product_group', product_group)

    if os.path.exists("/opt/airflow/dags/data/tmp/product_group_type.parquet"):
        product_group_type = pd.read_parquet("/opt/airflow/dags/data/tmp/product_group_type.parquet")
        if not product_group_type.empty:
            client.insert_df('raw_proxima.product_group_type', product_group_type)
            