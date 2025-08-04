import os
import pandas as pd
from utils import download_object, get_date_range, client_connect


# Download products from API
def download_workspace():
   
    url = "https://smartup.online/b/anor/mxsx/mrf/room$export"

    begin_date, end_date = get_date_range()

    obj_list = download_object(url, 'room', begin_date, end_date)

    if not obj_list:
        print("No inventory data found.")
        room = pd.DataFrame()
        room.to_parquet("/opt/airflow/dags/data/tmp/room.parquet", index=False)
    else:
        room = pd.json_normalize(obj_list)
        
        room.to_parquet("/opt/airflow/dags/data/tmp/room.parquet", index=False)

    

## Load data into ClickHouse
def load_workspace_clickhouse():

    # ClickHouse server connection
    client = client_connect()

    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.workspace (
        room_id String,
        room_code Nullable(String),
        filial_code Nullable(String),
        room_name Nullable(String),
        room_kind Nullable(String),
        room_type_code Nullable(String),
        state Nullable(String),
        order_no Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY room_id;
    ''')


    if os.path.exists("/opt/airflow/dags/data/tmp/room.parquet"):
        room = pd.read_parquet("/opt/airflow/dags/data/tmp/room.parquet")
        if not room.empty:
            client.insert_df('raw_proxima.workspace', room)


