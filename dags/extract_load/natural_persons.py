import os
import pandas as pd
from utils import download_object, get_date_range, client_connect


## Download data from API
def download_natural_person():
        
    url = "https://smartup.online/b/anor/mxsx/mr/natural_person$export"

    begin_date, end_date = get_date_range() 

    obj_list = download_object(url, 'natural_person', begin_date, end_date)


    if not obj_list:
        print("No data found for the specified date range.")
        LegalEntity = pd.DataFrame()
        room = pd.DataFrame()  
        group = pd.DataFrame()
        LegalEntity.to_parquet("/opt/airflow/dags/data/tmp/natural_person.parquet", index=False)
        room.to_parquet("/opt/airflow/dags/data/tmp/natural_person_room.parquet", index=False)
        group.to_parquet("/opt/airflow/dags/data/tmp/natural_person_group.parquet", index=False)
    else:
        natural_person = pd.json_normalize(obj_list).drop(columns=['rooms', 'groups'])
        rooms = []
        for item in obj_list:
            if item['rooms']:
                for room in item['rooms']:
                    room['person_id'] = item['person_id']
                    rooms.append(room)
        rooms = pd.json_normalize(rooms)

        groups = []
        for item in obj_list:
            if item['groups']:
                for group in item['groups']:
                    group['person_id'] = item['person_id']
                    groups.append(group)
        groups = pd.json_normalize(groups)

        natural_person.to_parquet("/opt/airflow/dags/data/tmp/natural_person.parquet", index=False)
        rooms.to_parquet("/opt/airflow/dags/data/tmp/natural_person_room.parquet", index=False)
        groups.to_parquet("/opt/airflow/dags/data/tmp/natural_person_group.parquet", index=False)



## Load data into ClickHouse
def load_natural_person_clickhouse():

    # ClickHouse server connection
    client = client_connect()

    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.natural_person (
        person_id String,
        first_name Nullable(String),
        last_name Nullable(String),
        middle_name Nullable(String),
        gender Nullable(String),
        birthday Nullable(String),
        latlng Nullable(String),
        code Nullable(String),
        web Nullable(String),
        main_phone Nullable(String),
        address Nullable(String),
        post_address Nullable(String),
        region_code Nullable(String),
        region_name Nullable(String),
        legal_person_code Nullable(String),
        telegram Nullable(String),
        email Nullable(String),
        is_budgetarian Nullable(String),
        is_client Nullable(String),
        is_supplier Nullable(String),
        state Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY person_id;
    ''')    

    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.natural_person_room (
        room_id String,
        person_id Nullable(String),
        room_code Nullable(String),
        room_type_code Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY room_id;
    ''')

    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.natural_person_group (
        group_code String,
        person_id Nullable(String),
        type_code Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY group_code;
    ''')
    
    # ✅ Read from saved files
    if os.path.exists("/opt/airflow/dags/data/tmp/natural_person.parquet"):
        legal_person = pd.read_parquet("/opt/airflow/dags/data/tmp/natural_person.parquet")
        if not legal_person.empty:
            client.insert_df('raw_proxima.natural_person', legal_person)

    if os.path.exists("/opt/airflow/dags/data/tmp/natural_person_room.parquet"):
        rooms = pd.read_parquet("/opt/airflow/dags/data/tmp/natural_person_room.parquet")
        if not rooms.empty:
            client.insert_df('raw_proxima.natural_person_room', rooms)
            
    if os.path.exists("/opt/airflow/dags/data/tmp/natural_person_group.parquet"):
        groups = pd.read_parquet("/opt/airflow/dags/data/tmp/natural_person_group.parquet")
        if not groups.empty:
            client.insert_df('raw_proxima.natural_person_group', groups)
            