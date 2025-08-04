import pandas as pd
import os
from datetime import timedelta
from utils import get_date_range, download_object, client_connect


url = "https://smartup.online/b/trade/txs/tdeal/order$export"

def download_order():

    # Boshlanish va tugash sanalari
    start_date, end_date = get_date_range()
    delta = timedelta(days=30)

    all_orders = []
    while start_date < end_date:
        range_start = start_date
        range_end = min(start_date + delta, end_date)

        obj_list = download_object(url, 'order', range_start, range_end)

        if obj_list:
            all_orders.extend(obj_list)

        start_date = range_end + timedelta(days=1)

    if all_orders:
        order_df = pd.json_normalize(all_orders).drop(columns=['order_products'])

        order_item_list = []
        for order in all_orders:
            # break
            order_products = order.get('order_products', [])

            if order_products:
                for product in order_products:
                    for detail in product.get('details', []):
                        item = product.copy()
                        item.update({
                            'batch_number': detail.get('batch_number'),
                            'card_code': detail.get('card_code'),
                            'sold_quant': detail.get('sold_quant'),
                            'expiry_date': detail.get('expiry_date'),
                            'deal_id': order.get('deal_id')
                        })
                        order_item_list.append(item)

        order_items_df = pd.json_normalize(order_item_list).drop(columns=['details'])

        order_df.to_parquet("/opt/airflow/dags/data/tmp/order.parquet", index=False)
        order_items_df.to_parquet("/opt/airflow/dags/data/tmp/order_item.parquet", index=False)

    else:
        order_df = pd.DataFrame()
        order_items_df = pd.DataFrame()
        order_df.to_parquet("/opt/airflow/dags/data/tmp/order.parquet", index=False)
        order_items_df.to_parquet("/opt/airflow/dags/data/tmp/order_item.parquet", index=False)



## Load data into ClickHouse
def load_order_clickhouse():

    # ClickHouse server connection
    client = client_connect()

    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.order (
        deal_id String,
        filial_id Nullable(String),
        filial_code Nullable(String),
        external_id Nullable(String),
        invoice_external_id Nullable(String),
        subfilial_code Nullable(String),
        deal_time Nullable(String),
        delivery_number Nullable(String),
        delivery_date Nullable(String),
        booked_date Nullable(String),
        total_amount Nullable(String),
        room_id Nullable(String),
        room_code Nullable(String),
        room_name Nullable(String),
        robot_code Nullable(String),
        lap_code Nullable(String),
        sales_manager_id Nullable(String),
        sales_manager_code Nullable(String),
        sales_manager_name Nullable(String),
        expeditor_id Nullable(String),
        expeditor_code Nullable(String),
        expeditor_name Nullable(String),
        person_id Nullable(String),
        person_code Nullable(String),
        person_name Nullable(String),
        person_local_code Nullable(String),
        person_latitude Nullable(String),
        person_longitude Nullable(String),
        person_tin Nullable(String),
        currency_code Nullable(String),
        owner_person_code Nullable(String),
        manager_code Nullable(String),
        van_code Nullable(String),
        contract_code Nullable(String),
        contract_number Nullable(String),
        invoice_number Nullable(String),
        payment_type_code Nullable(String),
        deal_margin_kind Nullable(String),
        deal_margin_value Nullable(String),
        visit_payment_type_code Nullable(String),
        note Nullable(String),
        deal_note Nullable(String),
        status Nullable(String),
        with_marking Nullable(String),
        self_shipment Nullable(String),
        delivery_address_short Nullable(String),
        delivery_address_full Nullable(String),
        marking_attaching_method Nullable(String),
        visit_id Nullable(String),
        modified_id Nullable(String),
        total_weight_netto Nullable(String),
        total_weight_brutto Nullable(String),
        total_litre Nullable(String),
        order_gifts Nullable(String),
        order_actions Nullable(String),
        order_consignments Nullable(String),
        return_reason_code Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY deal_id;
    ''')
    
    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.order_item (
        product_unit_id String,
        deal_id Nullable(String),
        external_id String,
        product_id Nullable(String),
        product_code Nullable(String),
        product_barcode Nullable(String),
        product_local_code Nullable(String),
        product_name Nullable(String),
        serial_number Nullable(String),
        inventory_kind Nullable(String),
        on_balance Nullable(String),
        warehouse_code Nullable(String),
        order_quant Nullable(String),
        return_quant Nullable(String),
        product_price Nullable(String),
        margin_amount Nullable(String),
        margin_value Nullable(String),
        margin_kind Nullable(String),
        vat_amount Nullable(String),
        vat_percent Nullable(String),
        price_type_code Nullable(String),
        price_type_id Nullable(String),
        expiry_date Nullable(String),
        card_code Nullable(String),
        batch_number Nullable(String),
        sold_quant Nullable(String),
        sold_amount Nullable(String),
    ) ENGINE = MergeTree()
    ORDER BY product_unit_id;
    ''')

    # ✅ Read from saved files
    if os.path.exists("/opt/airflow/dags/data/tmp/order.parquet"):
        order = pd.read_parquet("/opt/airflow/dags/data/tmp/order.parquet").astype(str)
        if not order.empty:
            client.insert_df('raw_proxima.order', order)

    if os.path.exists("/opt/airflow/dags/data/tmp/order_item.parquet"):
        order_item = pd.read_parquet("/opt/airflow/dags/data/tmp/order_item.parquet").astype(str)
        if not order_item.empty:
            client.insert_df('raw_proxima.order_item', order_item)
