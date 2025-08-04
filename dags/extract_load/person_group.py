import os
import pandas as pd
from utils import download_object, get_date_range, client_connect


## Download data from API
def download_person_group():
        
    url = "https://smartup.online/b/anor/mxsx/mr/person_group$export"

    begin_date, end_date = get_date_range() 

    obj_list = download_object(url, 'person_group', begin_date, end_date)

    if not obj_list:
        print("No person group data found.")
        person_group = pd.DataFrame()
        person_group_type = pd.DataFrame()
        person_group.to_parquet("/opt/airflow/dags/data/tmp/person_group.parquet", index=False)
        person_group_type.to_parquet("/opt/airflow/dags/data/tmp/person_group_type.parquet", index=False)
    else:
        person_group = pd.json_normalize(obj_list).drop(columns=['person_group_types'])

        person_list = []
        for i in obj_list:
            if i['person_group_types']:
                for person in i['person_group_types']:
                    person['person_group_id'] = i['person_group_id']
                    person_list.append(person)

        person_group_type = pd.json_normalize(person_list)

        person_group.to_parquet("/opt/airflow/dags/data/tmp/person_group.parquet", index=False)
        person_group_type.to_parquet("/opt/airflow/dags/data/tmp/person_group_type.parquet", index=False)


## Load data into ClickHouse
def load_person_group_clickhouse():

    # ClickHouse server connection
    client = client_connect()

    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.person_group (
            person_group_id String,
            code Nullable(String),
            name String,
            person_kind Nullable(String),
            state Nullable(String)
        ) ENGINE = MergeTree()
        ORDER BY person_group_id;
    ''')    

    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.person_group_type (
            person_type_id String,
            person_group_id Nullable(String),
            code Nullable(String),
            name String,
            order_no Nullable(String),
            state Nullable(String)
        ) ENGINE = MergeTree()
        ORDER BY person_type_id;
    ''')
    
    # ✅ Read from saved files
    if os.path.exists("/opt/airflow/dags/data/tmp/person_group.parquet"):
        person_group = pd.read_parquet("/opt/airflow/dags/data/tmp/person_group.parquet")
        if not person_group.empty:
            client.insert_df('raw_proxima.person_group', person_group)

    if os.path.exists("/opt/airflow/dags/data/tmp/person_group_type.parquet"):
        person_group_type = pd.read_parquet("/opt/airflow/dags/data/tmp/person_group_type.parquet")
        if not person_group_type.empty:
            client.insert_df('raw_proxima.person_group_type', person_group_type)
