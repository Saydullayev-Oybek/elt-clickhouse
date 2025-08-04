import os
import pandas as pd
from utils import client_connect

## Download data from API
def download_company():
        
        if os.path.exists("/opt/airflow/dags/data/sm_f/companies.xlsx"):

            companies = pd.read_excel("/opt/airflow/dags/data/sm_f/companies.xlsx").rename(
                columns={
                    'ИД организации': 'organization_id',
                    'ИНН': 'inn',
                    'Название': 'name',
                    'Статус': 'status',
                    'Дата': 'date',
                    'Кол-во пользователей': 'number_users',
                    'Email': 'email',
                    'Дата изменения': 'date_modification',
                    'Изменил': 'changed_by',
                    'Код': 'code',
                    'НДС (%)': 'vat',
                    'Пользователи': 'users',
                    'Регион': 'region',
                    'Создал': 'created_by',
                    'Телефон': 'phone',
                    'Юридическое лицо': 'legal_entity',
                    '№': 'number'
                    }
            )

            if os.path.exists("/opt/airflow/dags/data/sm_f_old/companies.csv"):
                companies_old = pd.read_csv("/opt/airflow/dags/data/sm_f_old/companies.csv")
                merged_companies = pd.merge(companies, companies_old, on='organization_id', how='left', indicator=True)
                merged_companies = merged_companies[merged_companies['_merge'] == 'left_only'].drop(columns=['_merge'])
                companies.to_csv("/opt/airflow/dags/data/sm_f_old/companies.csv", index=False)
                merged_companies.to_csv("/opt/airflow/dags/data/sm_f/companies_to_load.csv", index=False)
            else:
                companies.to_csv("/opt/airflow/dags/data/sm_f_old/companies.csv", index=False)
                companies.to_csv("/opt/airflow/dags/data/sm_f/companies_to_load.csv", index=False)


## Load data into ClickHouse
def load_company_clickhouse():

    # ClickHouse server connection
    client = client_connect()

    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.company(
            organization_id String,
            inn Nullable(String),
            name Nullable(String),
            status Nullable(String),
            date Nullable(String),
            number_users Nullable(String),
            email Nullable(String),
            date_modification Nullable(String),
            changed_by Nullable(String),
            code Nullable(String),
            vat Nullable(String),
            users Nullable(String),
            region Nullable(String),
            created_by Nullable(String),
            phone Nullable(String),
            legal_entity Nullable(String),
            number Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY organization_id;
    ''')    

    
    # ✅ Read from saved files
    if os.path.exists("/opt/airflow/dags/data/sm_f/companies_to_load.csv"):
        companies = pd.read_csv("/opt/airflow/dags/data/sm_f/companies_to_load.csv").astype(str)
        if not companies.empty:
            client.insert_df('raw_proxima.company', companies)


