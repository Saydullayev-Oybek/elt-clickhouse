import os
import pandas as pd
from utils import download_object, get_date_range, client_connect

def download_writeoff():
        
    url = "https://smartup.online/b/anor/mxsx/mkw/writeoff$export"

    begin_date, end_date = get_date_range()
    obj_list = download_object(url, 'writeoff', begin_date, end_date)

    if not obj_list:
        print("No product group data found.")
        writeoff = pd.DataFrame()
        writeoff_item = pd.DataFrame()
        writeoff.to_parquet("/opt/airflow/dags/data/tmp/writeoff.parquet", index=False)
        writeoff_item.to_parquet("/opt/airflow/dags/data/tmp/writeoff_item.parquet", index=False)
    else:
        writeoff = pd.json_normalize(obj_list).drop(columns=['writeoff_items'])


        writeoff_item_list = []
        for ret in obj_list:
            # break
            writeoff_item = ret.get('writeoff_items', [])

            if writeoff_item:
                for i in writeoff_item:
                    item = i.copy()
                    item.update({
                        'writeoff_id': ret.get('writeoff_id')
                    })
                    writeoff_item_list.append(item)

        writeoff_item = pd.json_normalize(writeoff_item_list)
        # Save data to parquet files
        writeoff.to_parquet("/opt/airflow/dags/data/tmp/writeoff.parquet", index=False)
        writeoff_item.to_parquet("/opt/airflow/dags/data/tmp/writeoff_item.parquet", index=False)


## Load data into ClickHouse
def load_writeoff_clickhouse():

    # ClickHouse server connection
    client = client_connect()

    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.writeoff (
        writeoff_id String,
        filial_code Nullable(String),
        external_id Nullable(String),
        status Nullable(String),
        writeoff_number Nullable(String),
        writeoff_date Nullable(String),
        currency_code Nullable(String),
        warehouse_code Nullable(String),
        reason_code Nullable(String),
        note Nullable(String),
        barcode Nullable(String),
        c_amount Nullable(String),
        c_amount_base Nullable(String)
    )   ENGINE = MergeTree()
        ORDER BY writeoff_id;
    ''')
    
    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.writeoff_item (
        writeoff_id String,
        external_id Nullable(String),
        writeoff_item_id Nullable(String),
        inventory_kind Nullable(String),
        product_code Nullable(String),
        serial_number Nullable(String),
        card_code Nullable(String),
        expiry_date Nullable(String),
        quantity Nullable(String),
        batch_number Nullable(String)
    )   ENGINE = MergeTree()
        ORDER BY writeoff_id;
    ''')

    # ✅ Read from saved files
    if os.path.exists("/opt/airflow/dags/data/tmp/writeoff.parquet"):
        writeoff = pd.read_parquet("/opt/airflow/dags/data/tmp/writeoff.parquet")
        if not writeoff.empty:
            client.insert_df('raw_proxima.writeoff', writeoff)

    if os.path.exists("/opt/airflow/dags/data/tmp/writeoff_item.parquet"):
        writeoff_item = pd.read_parquet("/opt/airflow/dags/data/tmp/writeoff_item.parquet")
        if not writeoff_item.empty:
            client.insert_df('raw_proxima.writeoff_item', writeoff_item)
            