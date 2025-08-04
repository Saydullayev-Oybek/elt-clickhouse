import os
import pandas as pd
from utils import client_connect

## Download data from API
def download_currency():
        
        if os.path.exists("/opt/airflow/dags/data/sm_f/currency.xlsx"):

            currency = pd.read_excel("/opt/airflow/dags/data/sm_f/currency.xlsx")

            if os.path.exists("/opt/airflow/dags/data/sm_f_old/currency.csv"):
                currency_old = pd.read_csv("/opt/airflow/dags/data/sm_f_old/currency.csv")
                max_date = currency_old['date'].max()
                currency_new = currency[currency['date'] > max_date]
                currency.to_csv("/opt/airflow/dags/data/sm_f_old/currency.csv", index=False)
                currency_new.to_csv("/opt/airflow/dags/data/sm_f/currency_to_load.csv", index=False)
            else:
                currency.to_csv("/opt/airflow/dags/data/sm_f_old/currency.csv", index=False)
                currency.to_csv("/opt/airflow/dags/data/sm_f/currency_to_load.csv", index=False)


## Load data into ClickHouse
def load_currency_clickhouse():

    # ClickHouse server connection
    client = client_connect()

    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.currency(
        date String,
        from_currency String,
        to_currency Nullable(String),
        rate Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY date;
    ''')    

    
    # ✅ Read from saved files
    if os.path.exists("/opt/airflow/dags/data/sm_f/currency_to_load.csv"):
        currency = pd.read_csv("/opt/airflow/dags/data/sm_f/currency_to_load.csv").astype(str)
        if not currency.empty:
            client.insert_df('raw_proxima.currency', currency)


