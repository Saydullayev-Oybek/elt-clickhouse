import os
import pandas as pd
from utils import download_object, get_date_range, client_connect


# Download products from API
def download_inventory():
   
    url = 'https://smartup.online/b/anor/mxsx/mr/inventory$export'

    begin_date, end_date = get_date_range()
    # end_date = datetime(2024, 11, 11)

    obj_list = download_object(url, 'inventory', begin_date, end_date)

    if not obj_list:
        print("No inventory data found.")
        inventory = pd.DataFrame()
        inventory_group = pd.DataFrame()
        inventory.to_parquet("/opt/airflow/dags/data/tmp/inventory.parquet", index=False)
        inventory_group.to_parquet("/opt/airflow/dags/data/tmp/inventory_group.parquet", index=False)
        # save_sync_date(end_date)
        # return 
    else:
        inventory_list = []
        for item in obj_list:
            # break    
            item['inventory_kind'] = item['inventory_kinds'][0]['inventory_kind']
            item['sector_code'] = item['sector_codes'][0]['sector_code']
            inventory_list.append(item)

        inventory = pd.json_normalize(inventory_list).drop(columns=['inventory_kinds', 'sector_codes', 'groups'])

        inventory_group = []
        for item in obj_list:
            if item['groups']:
                for group in item['groups']:
                    group['product_id'] = item['product_id']
                    inventory_group.append(group)
        inventory_group = pd.json_normalize(inventory_group)
        
        # return inventory, inventory_group

        # ✅ Save data to files
        inventory.to_parquet("/opt/airflow/dags/data/tmp/inventory.parquet", index=False)
        inventory_group.to_parquet("/opt/airflow/dags/data/tmp/inventory_group.parquet", index=False)

        # save_sync_date(end_date)
    

## Load data into ClickHouse
def load_inventory_clickhouse():

    # ClickHouse server connection
    client = client_connect()

    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.inventory (
        product_id String,
        code Nullable(String),
        name Nullable(String),
        short_name Nullable(String),
        weight_netto Nullable(String),
        weight_brutto Nullable(String),
        litr Nullable(String),
        box_type_code Nullable(String),
        box_quant Nullable(Int32),
        producer_code Nullable(String),
        measure_code Nullable(String),
        state Nullable(String),
        order_no Nullable(String),
        article_code Nullable(String),
        barcodes Nullable(String),
        gtin Nullable(String),
        ikpu Nullable(String),
        tnved Nullable(String),
        marking_group_code Nullable(String),
        inventory_kind Nullable(String),
        sector_code Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY product_id;
    ''')
    
    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.inventory_group (
        group_id String,
        product_id String,
        group_code Nullable(String),
        type_id Nullable(String),
        type_code Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY product_id;
    ''')
    # ✅ Read from saved files
    if os.path.exists("/opt/airflow/dags/data/tmp/inventory.parquet"):
        inventory = pd.read_parquet("/opt/airflow/dags/data/tmp/inventory.parquet")
        if not inventory.empty:
            client.insert_df('raw_proxima.inventory', inventory)

    if os.path.exists("/opt/airflow/dags/data/tmp/inventory_group.parquet"):
        inventory_group = pd.read_parquet("/opt/airflow/dags/data/tmp/inventory_group.parquet")
        if not inventory_group.empty:
            client.insert_df('raw_proxima.inventory_group', inventory_group)
