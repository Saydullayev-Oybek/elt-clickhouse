import os
import pandas as pd
from utils import client_connect


## Download data from API
def download_accounts():
        
        if os.path.exists("/opt/airflow/dags/data/sm_f/accounts.xlsx"):

            accounts = pd.read_excel("/opt/airflow/dags/data/sm_f/accounts.xlsx").rename(
                columns={
                    'ИД': 'id',
                    'Код': 'code',
                    'Название': 'name',
                    'Вид счета': 'account_type',
                    'Вид валюты': 'type_currency',
                    'Статус': 'status',
                    'Балансовый': 'balance',
                    'Дата изменения': 'date_modification',
                    'Дата создания': 'date_creation',
                    'Изменил': 'changed',
                    'Количественный': 'quantitative',
                    'Подчинен счету': 'subject_account',
                    'Полное название': 'full_name',
                    'Проверка на превышение баланса': 'check_excess_balance',
                    'Создал': 'created_by'
               }
            )

            if os.path.exists("/opt/airflow/dags/data/sm_f_old/accounts.csv"):
                accounts_old = pd.read_csv("/opt/airflow/dags/data/sm_f_old/accounts.csv")
                merged_accounts = pd.merge(accounts, accounts_old, on='id', how='left', indicator=True)
                merged_accounts = merged_accounts[merged_accounts['_merge'] == 'left_only'].drop(columns=['_merge'])
                accounts.to_csv("/opt/airflow/dags/data/sm_f_old/accounts.csv", index=False)
                merged_accounts.to_csv("/opt/airflow/dags/data/sm_f/accounts_to_load.csv", index=False)
            else:
                accounts.to_csv("/opt/airflow/dags/data/sm_f_old/accounts.csv", index=False)
                accounts.to_csv("/opt/airflow/dags/data/sm_f/accounts_to_load.csv", index=False)


## Load data into ClickHouse
def load_accounts_clickhouse():

    # ClickHouse server connection
    client = client_connect()

    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.account(
            id String,
            code Nullable(String),
            name Nullable(String),
            account_type Nullable(String),
            type_currency Nullable(String),
            status Nullable(String),
            balance Nullable(String),
            date_modification Nullable(String),
            date_creation Nullable(String),
            changed Nullable(String),
            quantitative Nullable(String),
            subject_account Nullable(String),
            full_name Nullable(String),
            check_excess_balance Nullable(String),
            created_by Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY id;
    ''')    

    
    # ✅ Read from saved files
    if os.path.exists("/opt/airflow/dags/data/sm_f/accounts_to_load.csv"):
        accounts = pd.read_csv("/opt/airflow/dags/data/sm_f/accounts_to_load.csv").astype(str)
        if not accounts.empty:
            client.insert_df('raw_proxima.account', accounts)


