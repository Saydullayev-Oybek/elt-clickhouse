import os
import pandas as pd
import clickhouse_connect
from utils import download_object, get_date_range, client_connect


## Download data from API
def download_legal_entity():
        
    url = "https://smartup.online/b/anor/mxsx/mr/legal_person$export"

    begin_date, end_date = get_date_range() 

    obj_list = download_object(url, 'legal_person', begin_date, end_date)


    if not obj_list:
        print("No data found for the specified date range.")
        LegalEntity = pd.DataFrame()
        rooms = pd.DataFrame()  
        groups = pd.DataFrame()
        LegalEntity.to_parquet("/opt/airflow/dags/data/tmp/legal_person.parquet", index=False)
        rooms.to_parquet("/opt/airflow/dags/data/tmp/legal_person_room.parquet", index=False)
        groups.to_parquet("/opt/airflow/dags/data/tmp/legal_person_group.parquet", index=False)
    else:
        legal_person = pd.json_normalize(obj_list).drop(columns=['rooms', 'groups'])
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

        legal_person.to_parquet("/opt/airflow/dags/data/tmp/legal_person.parquet", index=False)
        rooms.to_parquet("/opt/airflow/dags/data/tmp/legal_person_room.parquet", index=False)
        groups.to_parquet("/opt/airflow/dags/data/tmp/legal_person_group.parquet", index=False)

## Load data into ClickHouse
def load_legal_entity_clickhouse():

    # ClickHouse server connection
    client = client_connect()

    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.legal_person (
        person_id String,
        name Nullable(String),
        short_name Nullable(String),
        code Nullable(String),
        latlng Nullable(String),
        tin Nullable(String),
        cea Nullable(String),
        main_phone Nullable(String),
        web Nullable(String),
        address Nullable(String),
        post_address Nullable(String),
        address_guide Nullable(String),
        region_id Nullable(String),
        region_code Nullable(String),
        primary_person_code Nullable(String),
        parent_person_code Nullable(String),
        allow_owner Nullable(String),
        vat_code Nullable(String),
        barcode Nullable(String),
        zip_code Nullable(String),
        email Nullable(String),
        is_budgetarian Nullable(String),
        is_client Nullable(String),
        is_supplier Nullable(String),
        state Nullable(String),
        bank_accounts Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY person_id;
    ''')    

    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.legal_person_room (
        room_id String,
        person_id Nullable(String),
        room_code Nullable(String),
        room_type_code Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY room_id;
    ''')

    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.legal_person_group (
        group_id String,
        person_id Nullable(String),
        group_code Nullable(String),
        type_id Nullable(String),
        type_code Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY group_id;
    ''')
    
    # ✅ Read from saved files
    if os.path.exists("/opt/airflow/dags/data/tmp/legal_person.parquet"):
        legal_person = pd.read_parquet("/opt/airflow/dags/data/tmp/legal_person.parquet").astype(str)
        if not legal_person.empty:
            client.insert_df('raw_proxima.legal_person', legal_person)

    if os.path.exists("/opt/airflow/dags/data/tmp/legal_person_room.parquet"):
        rooms = pd.read_parquet("/opt/airflow/dags/data/tmp/legal_person_room.parquet").astype(str)
        if not rooms.empty:
            client.insert_df('raw_proxima.legal_person_room', rooms)
            
    if os.path.exists("/opt/airflow/dags/data/tmp/legal_person_group.parquet"):
        groups = pd.read_parquet("/opt/airflow/dags/data/tmp/legal_person_group.parquet").astype(str)
        if not groups.empty:
            client.insert_df('raw_proxima.legal_person_group', groups)