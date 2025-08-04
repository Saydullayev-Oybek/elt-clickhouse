import os
import pandas as pd
from utils import download_object, get_date_range, client_connect

def download_return():

    url = 'https://smartup.online/b/anor/mxsx/mdeal/return$export'  # To‘g‘ri endpointni shu yerga yozing
    
    begin_date, end_date = get_date_range()

    obj_list = download_object(url, 'return', begin_date, end_date)

    if obj_list:
        return_df = pd.json_normalize(obj_list).drop(columns=['return_products'])

        return_item_list = []
        for ret in obj_list:
            # break
            return_products = ret.get('return_products', [])

            if return_products:
                for i in return_products:
                    item = i.copy()
                    item.update({
                        'deal_id': ret.get('deal_id')
                    })
                    return_item_list.append(item)

        return_items_df = pd.json_normalize(return_item_list)

        return_df.to_parquet("/opt/airflow/dags/data/tmp/return.parquet", index=False)
        return_items_df.to_parquet("/opt/airflow/dags/data/tmp/return_item.parquet", index=False)

    else:
        return_df = pd.DataFrame()
        return_items_df = pd.DataFrame()
        return_df.to_parquet("/opt/airflow/dags/data/tmp/return.parquet", index=False)
        return_items_df.to_parquet("/opt/airflow/dags/data/tmp/return_item.parquet", index=False)

## Load data into ClickHouse
def load_return_clickhouse():

    # ClickHouse server connection
    client = client_connect()

    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.return (
        deal_id String,
        filial_code Nullable(String),
        external_id Nullable(String),
        subfilial_code Nullable(String),
        deal_time Nullable(String),
        order_deal_id Nullable(String),
        delivery_date Nullable(String),
        delivery_number Nullable(String),
        booked_date Nullable(String),
        room_id Nullable(String),
        room_code Nullable(String),
        robot_code Nullable(String),
        sales_manager_code Nullable(String),
        sales_manager_name Nullable(String),
        expeditor_code Nullable(String),
        person_code Nullable(String),
        person_id Nullable(String),
        person_name Nullable(String),
        person_tin Nullable(String),
        currency_code Nullable(String),
        owner_person_code Nullable(String),
        manager_code Nullable(String),
        van_code Nullable(String),
        contract_code Nullable(String),
        invoice_number Nullable(String),
        batch_number Nullable(String),
        payment_type_code Nullable(String),
        total_amount Nullable(String),
        note Nullable(String),
        status Nullable(String),
        return_reason_id Nullable(String),
        return_reason_code Nullable(String)
    ) 
        ENGINE = MergeTree()
        ORDER BY deal_id;
    ''')


    
    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.return_item (
        deal_id String,
        product_unit_id Nullable(String),
        external_id Nullable(String),
        product_code Nullable(String),
        product_name Nullable(String),
        serial_number Nullable(String),
        expiry_date Nullable(String),
        return_quant Nullable(String),
        product_price Nullable(String),
        margin_amount Nullable(String),
        margin_value Nullable(String),
        margin_kind Nullable(String),
        card_code Nullable(String),
        vat_percent Nullable(String),
        vat_amount Nullable(String),
        sold_amount Nullable(String),
        inventory_kind Nullable(String),
        on_balance Nullable(String),
        price_type_code Nullable(String),
        warehouse_code Nullable(String)
    ) 
        ENGINE = MergeTree()
        ORDER BY deal_id;
    ''')
    
    # ✅ Read from saved files
    if os.path.exists("/opt/airflow/dags/data/tmp/return.parquet"):
        returns = pd.read_parquet("/opt/airflow/dags/data/tmp/return.parquet").astype(str)
        if not returns.empty:
            client.insert_df('raw_proxima.return', returns)
    if os.path.exists("/opt/airflow/dags/data/tmp/return_item.parquet"):
        return_item = pd.read_parquet("/opt/airflow/dags/data/tmp/return_item.parquet").astype(str)
        if not return_item.empty:
            client.insert_df('raw_proxima.return_item', return_item)
