import os
import pandas as pd
from utils import download_object, get_date_range, client_connect

def download_return_supplier():
        
    url = "https://smartup.online/b/anor/mxsx/mkw/return$export"

    begin_date, end_date = get_date_range()
    obj_list = download_object(url, 'return', begin_date, end_date)

    if not obj_list:
        print("No product group data found.")
        return_supplier = pd.DataFrame()
        return_supplier_item = pd.DataFrame()
        return_supplier.to_parquet("/opt/airflow/dags/data/tmp/return_supplier.parquet", index=False)
        return_supplier_item.to_parquet("/opt/airflow/dags/data/tmp/return_supplier_item.parquet", index=False)
    else:
        return_supplier = pd.json_normalize(obj_list).drop(columns=['return_items'])


        return_supplier_item_list = []
        for ret in obj_list:
            # break
            return_supplier_item = ret.get('return_items', [])

            if return_supplier_item:
                for i in return_supplier_item:
                    item = i.copy()
                    item.update({
                        'return_id': ret.get('return_id')
                    })
                    return_supplier_item_list.append(item)

        return_supplier_item = pd.json_normalize(return_supplier_item_list)
        # Save data to parquet files
        return_supplier.to_parquet("/opt/airflow/dags/data/tmp/return_supplier.parquet", index=False)
        return_supplier_item.to_parquet("/opt/airflow/dags/data/tmp/return_supplier_item.parquet", index=False)


## Load data into ClickHouse
def load_return_supplier_clickhouse():

    # ClickHouse server connection
    client = client_connect()

    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.return_supplier (
        return_id String,
        external_id Nullable(String),
        filial_code Nullable(String),
        return_time Nullable(String),
        return_number Nullable(String),
        currency_code Nullable(String),
        warehouse_code Nullable(String),
        supplier_code Nullable(String),
        reason_code Nullable(String),
        purchase_id Nullable(String),
        note Nullable(String),
        owner_person_code Nullable(String),
        contract_code Nullable(String),
        invoice_number Nullable(String),
        invoice_date Nullable(String),
        status Nullable(String)
    )   ENGINE = MergeTree()
        ORDER BY return_id;
    ''')
    
    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.return_supplier_item (
        return_id String,
        return_item_id Nullable(String),
        external_id Nullable(String),
        purchase_item_id Nullable(String),
        input_id Nullable(String),
        input_item_id Nullable(String),
        serial_number Nullable(String),
        inventory_kind Nullable(String),
        on_balance Nullable(String),
        product_code Nullable(String),
        card_code Nullable(String),
        expiry_date Nullable(String),
        batch_number Nullable(String),
        quantity Nullable(String),
        price Nullable(String),
        margin_kind Nullable(String),
        margin_value Nullable(String),
        vat_percent Nullable(String),
        vat_amount Nullable(String)
    )   ENGINE = MergeTree()
        ORDER BY return_id;
    ''')

    # ✅ Read from saved files
    if os.path.exists("/opt/airflow/dags/data/tmp/return_supplier.parquet"):
        return_supplier = pd.read_parquet("/opt/airflow/dags/data/tmp/return_supplier.parquet")
        if not return_supplier.empty:
            client.insert_df('raw_proxima.return_supplier', return_supplier)

    if os.path.exists("/opt/airflow/dags/data/tmp/return_supplier_item.parquet"):
        return_supplier_item = pd.read_parquet("/opt/airflow/dags/data/tmp/return_supplier_item.parquet")
        if not return_supplier_item.empty:
            client.insert_df('raw_proxima.return_supplier_item', return_supplier_item)
            