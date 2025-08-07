import os
import pandas as pd
from utils import download_object, get_date_range, client_connect

# Download products from API
def download_bank_operation():
   
    url = "https://smartup.online/b/anor/mxsx/mkcs/bank_operation$export"

    begin_date, end_date = get_date_range()
    # end_date = datetime(2024, 11, 11)

    obj_list = download_object(url, 'bank_operation', begin_date, end_date)

    if not obj_list:
        print("No bank operation data found.")
        bank_operation = pd.DataFrame()
        bank_operation_ref_code = pd.DataFrame()
        bank_operation.to_parquet("/opt/airflow/dags/data/tmp/bank_operation.parquet", index=False)
        bank_operation_ref_code.to_parquet("/opt/airflow/dags/data/tmp/bank_operation_ref_code.parquet", index=False)
        # save_sync_date(end_date)
        # return 
    else:
        bank_operation = pd.json_normalize(obj_list).drop(columns=['ref_codes'])

        ref_code_list = []
        for i in obj_list:
            # break
            if i['ref_codes']:
                for j in i['ref_codes']:
                    j['operation_id'] = i['operation_id']
                    ref_code_list.append(j)

        ref_code_df = pd.json_normalize(ref_code_list)

        
        # ✅ Save data to files
        bank_operation.to_parquet("/opt/airflow/dags/data/tmp/bank_operation.parquet", index=False)
        ref_code_df.to_parquet("/opt/airflow/dags/data/tmp/bank_operation_ref_code.parquet", index=False)

        # save_sync_date(end_date)
    

## Load data into ClickHouse
def load_bank_operation_clickhouse():

    # ClickHouse server connection
    client = client_connect()

    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.bank_operation (
        operation_date String,
        external_id Nullable(String),
        operation_id Nullable(String),
        filial_code Nullable(String),
        operation_number Nullable(String),
        subfilial_code Nullable(String),
        posted Nullable(String),
        bank_trans_number Nullable(String),
        bank_trans_date Nullable(String),
        bank_account_code Nullable(String),
        cashflow_reason_code Nullable(String),
        cashflow_kind Nullable(String),
        corr_coa_code Nullable(String),
        corr_person_code Nullable(String),
        corr_bank_account_code Nullable(String),
        currency_code Nullable(String),
        amount Nullable(String),
        payment_code Nullable(String),
        purpose Nullable(String),
        responsible_person_code Nullable(String),
        note Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY operation_date;
    ''')
    
    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.bank_operation_ref_code (
        ref_id String,
        ref_type Nullable(String),
        operation_id Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY ref_id;
    ''')
    # ✅ Read from saved files
    if os.path.exists("/opt/airflow/dags/data/tmp/bank_operation.parquet"):
        bank_operation = pd.read_parquet("/opt/airflow/dags/data/tmp/bank_operation.parquet")
        if not bank_operation.empty:
            client.insert_df('raw_proxima.bank_operation', bank_operation)

    if os.path.exists("/opt/airflow/dags/data/tmp/bank_operation_ref_code.parquet"):
        bank_operation_ref_code = pd.read_parquet("/opt/airflow/dags/data/tmp/bank_operation_ref_code.parquet")
        if not bank_operation_ref_code.empty:
            client.insert_df('raw_proxima.bank_operation_ref_code', bank_operation_ref_code)
