import os
import pandas as pd
from utils import download_object, get_date_range, client_connect

def download_movement():
        
    url = "https://smartup.online/b/anor/mxsx/mkw/movement$export"

    begin_date, end_date = get_date_range()
    # begin_date = datetime(2024, 9, 6)
    obj_list = download_object(url, 'movement', begin_date, end_date)

    if not obj_list:
        print("No product group data found.")
        movement = pd.DataFrame()
        movement_item = pd.DataFrame()
        movement.to_parquet("/opt/airflow/dags/data/tmp/movement.parquet", index=False)
        movement_item.to_parquet("/opt/airflow/dags/data/tmp/movement_item.parquet", index=False)
    else:
        movement = pd.json_normalize(obj_list).drop(columns=['movement_items'])


        movement_item_list = []
        for ret in obj_list:
            # break
            movement_item = ret.get('movement_items', [])

            if movement_item:
                for i in movement_item:
                    item = i.copy()
                    item.update({
                        'movement_id': ret.get('movement_id')
                    })
                    movement_item_list.append(item)

        movement_item = pd.json_normalize(movement_item_list)
        # Save data to parquet files
        movement.to_parquet("/opt/airflow/dags/data/tmp/movement.parquet", index=False)
        movement_item.to_parquet("/opt/airflow/dags/data/tmp/movement_item.parquet", index=False)


## Load data into ClickHouse
def load_movement_clickhouse():

    # ClickHouse server connection
    client = client_connect()

    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.movement (
        movement_id String,
        filial_code Nullable(String),
        external_id Nullable(String),
        status Nullable(String),
        movement_number Nullable(String),
        from_movement_date Nullable(String),
        to_movement_date Nullable(String),
        request_id Nullable(String),
        from_warehouse_code Nullable(String),
        to_warehouse_code Nullable(String),
        reason_code Nullable(String),
        note Nullable(String),
        barcode Nullable(String)
    )   ENGINE = MergeTree()
        ORDER BY movement_id;
    ''')
    
    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.movement_item (
        movement_id String,
        external_id Nullable(String),
        movement_item_id Nullable(String),
        request_item_id Nullable(String),
        product_code Nullable(String),
        serial_number Nullable(String),
        inventory_kind Nullable(String),
        on_balance Nullable(String),
        card_code Nullable(String),
        expiry_date Nullable(String),
        quantity Nullable(String),
        batch_number Nullable(String)
    )   ENGINE = MergeTree()
        ORDER BY movement_id;
    ''')

    # ✅ Read from saved files
    if os.path.exists("/opt/airflow/dags/data/tmp/movement.parquet"):
        movement = pd.read_parquet("/opt/airflow/dags/data/tmp/movement.parquet")
        if not movement.empty:
            client.insert_df('raw_proxima.movement', movement)

    if os.path.exists("/opt/airflow/dags/data/tmp/movement_item.parquet"):
        movement_item = pd.read_parquet("/opt/airflow/dags/data/tmp/movement_item.parquet")
        if not movement_item.empty:
            client.insert_df('raw_proxima.movement_item', movement_item)
            