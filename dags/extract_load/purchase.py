import pandas as pd
import os
from datetime import timedelta
from utils import get_date_range, download_object, client_connect


url = "https://smartup.online/b/anor/mxsx/mkw/purchase$export"

def download_purchase():

    # Boshlanish va tugash sanalari
    start_date, end_date = get_date_range()
    delta = timedelta(days=60)

    all_purchases = []
    while start_date < end_date:
        range_start = start_date
        range_end = min(start_date + delta, end_date)

        obj_list = download_object(url, 'purchase', range_start, range_end)

        if obj_list:
            all_purchases.extend(obj_list)

        start_date = range_end + timedelta(days=1)

    if all_purchases:
        purchase_df = pd.json_normalize(all_purchases).drop(columns=['purchase_items'])

        purchase_item_list = []
        for purchase in all_purchases:
            # break
            purchase_products = purchase.get('purchase_items', [])

            if purchase_products:
                for i in purchase_products:
                    item = i.copy()
                    item.update({
                        'purchase_id': purchase.get('purchase_id')
                    })
                    purchase_item_list.append(item)

        purchase_items_df = pd.json_normalize(purchase_item_list)

        purchase_df.to_parquet("/opt/airflow/dags/data/tmp/purchase.parquet", index=False)
        purchase_items_df.to_parquet("/opt/airflow/dags/data/tmp/purchase_item.parquet", index=False)

    else:
        purchase_df = pd.DataFrame()
        purchase_items_df = pd.DataFrame()
        purchase_df.to_parquet("/opt/airflow/dags/data/tmp/purchase.parquet", index=False)
        purchase_items_df.to_parquet("/opt/airflow/dags/data/tmp/purchase_item.parquet", index=False)



## Load data into ClickHouse
def load_purchase_clickhouse():

    # ClickHouse server connection
    client = client_connect()

    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.purchase (
        purchase_id String,
        filial_code Nullable(String),
        external_id Nullable(String),
        purchase_time Nullable(String),
        purchase_number Nullable(String),
        input_date Nullable(String),
        order_id Nullable(String),
        supplier_code Nullable(String),
        contract_code Nullable(String),
        invoice_number Nullable(String),
        invoice_date Nullable(String),
        invoice_external_id Nullable(String),
        total_margin_kind Nullable(String),
        total_margin_value Nullable(String),
        currency_code Nullable(String),
        note Nullable(String),
        warehouse_code Nullable(String),
        status_code Nullable(String),
        posted Nullable(String),
        batch_number Nullable(String)
    )   ENGINE = MergeTree()
        ORDER BY purchase_id;
    ''')
    
    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.purchase_item (
        purchase_id String,
        external_id Nullable(String),
        purchase_item_id Nullable(String),
        order_item_id Nullable(String),
        product_code Nullable(String),
        inventory_kind Nullable(String),
        on_balance Nullable(String),
        serial_number Nullable(String),
        card_code Nullable(String),
        expiry_date Nullable(String),
        base_price Nullable(String),
        quantity Nullable(String),
        price Nullable(String),
        margin_kind Nullable(String),
        margin_value Nullable(String),
        vat_percent Nullable(String),
        vat_amount Nullable(String),
        marking_codes Nullable(String)
    )   ENGINE = MergeTree()
        ORDER BY purchase_id;
    ''')

    # ✅ Read from saved files
    if os.path.exists("/opt/airflow/dags/data/tmp/purchase.parquet"):
        purchase = pd.read_parquet("/opt/airflow/dags/data/tmp/purchase.parquet").astype(str)
        if not purchase.empty:
            client.insert_df('raw_proxima.purchase', purchase)

    if os.path.exists("/opt/airflow/dags/data/tmp/purchase_item.parquet"):
        purchase_item = pd.read_parquet("/opt/airflow/dags/data/tmp/purchase_item.parquet").astype(str)
        if not purchase_item.empty:
            client.insert_df('raw_proxima.purchase_item', purchase_item)
