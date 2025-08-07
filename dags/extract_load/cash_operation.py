import os
import pandas as pd
from utils import download_object, get_date_range, client_connect

# Download products from API
def download_cash_operation():
   
    url = "https://smartup.online/b/anor/mxsx/mkcs/cash_operation$export"

    begin_date, end_date = get_date_range()
    # end_date = datetime(2024, 11, 11)

    obj_list = download_object(url, 'cash_operation', begin_date, end_date)

    if not obj_list:
        print("No bank operation data found.")
        cash_operation = pd.DataFrame()
        cash_operation_ref_code = pd.DataFrame()
        cash_operation.to_parquet("/opt/airflow/dags/data/tmp/cash_operation.parquet", index=False)
        cash_operation_ref_code.to_parquet("/opt/airflow/dags/data/tmp/cash_operation_ref_code.parquet", index=False)
        # save_sync_date(end_date)
        # return 
    else:
        cash_operation = pd.json_normalize(obj_list).drop(columns=['ref_codes'])

        ref_code_list = []
        for i in obj_list:
            # break
            if i['ref_codes']:
                for j in i['ref_codes']:
                    j['operation_id'] = i['operation_id']
                    ref_code_list.append(j)

        ref_code_df = pd.json_normalize(ref_code_list)

        
        # ✅ Save data to files
        cash_operation.to_parquet("/opt/airflow/dags/data/tmp/cash_operation.parquet", index=False)
        ref_code_df.to_parquet("/opt/airflow/dags/data/tmp/cash_operation_ref_code.parquet", index=False)

        # save_sync_date(end_date)
    

## Load data into ClickHouse
def load_cash_operation_clickhouse():

    # ClickHouse server connection
    client = client_connect()

    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.cash_operation (
        operation_date String,
        filial_code Nullable(String),
        external_id Nullable(String),
        operation_id Nullable(String),
        operation_number Nullable(String),
        subfilial_code Nullable(String),
        posted Nullable(String),
        cashbox_code Nullable(String),
        cashflow_reason_code Nullable(String),
        cashflow_kind Nullable(String),
        corr_coa_code Nullable(String),
        corr_person_code Nullable(String),
        currency_code Nullable(String),
        amount Nullable(String),
        responsible_person_code Nullable(String),
        collector_code Nullable(String),
        note Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY operation_date;
    ''')
    
    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.cash_operation_ref_code (
        ref_id String,
        ref_type Nullable(String),
        operation_id Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY ref_id;
    ''')
    # ✅ Read from saved files
    if os.path.exists("/opt/airflow/dags/data/tmp/cash_operation.parquet"):
        cash_operation = pd.read_parquet("/opt/airflow/dags/data/tmp/cash_operation.parquet")
        if not cash_operation.empty:
            client.insert_df('raw_proxima.cash_operation', cash_operation)

    if os.path.exists("/opt/airflow/dags/data/tmp/cash_operation_ref_code.parquet"):
        cash_operation_ref_code = pd.read_parquet("/opt/airflow/dags/data/tmp/cash_operation_ref_code.parquet")
        if not cash_operation_ref_code.empty:
            client.insert_df('raw_proxima.cash_operation_ref_code', cash_operation_ref_code)
