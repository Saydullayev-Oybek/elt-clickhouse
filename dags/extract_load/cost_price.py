import os
import pandas as pd
from utils import client_connect

## Download data from API
def download_cost_price():
        
        if os.path.exists("/opt/airflow/dags/data/sm_f/total_cost_price.xlsx"):

            total_cost_price = pd.read_excel("/opt/airflow/dags/data/sm_f/total_cost_price.xlsx")[3:]
            total_cost_price.columns.values[0] = 'order_id'
            total_cost_price.columns.values[1] = 'type_return'
            total_cost_price.columns.values[2] = 'cost_price'

            if os.path.exists("/opt/airflow/dags/data/sm_f_old/total_cost_price.csv"):
                total_cost_price_old = pd.read_csv("/opt/airflow/dags/data/sm_f_old/total_cost_price.csv")
                merged_total_cost_price = pd.merge(total_cost_price, total_cost_price_old, on=['order_id'], how='left', indicator=True)
                merged_total_cost_price = merged_total_cost_price[merged_total_cost_price['_merge'] == 'left_only'].drop(columns=['_merge'])
                total_cost_price.to_csv("/opt/airflow/dags/data/sm_f_old/total_cost_price.csv", index=False)
                merged_total_cost_price.to_csv("/opt/airflow/dags/data/sm_f/total_cost_price_to_load.csv", index=False)
            else:
                total_cost_price.to_csv("/opt/airflow/dags/data/sm_f_old/total_cost_price.csv", index=False)
                total_cost_price.to_csv("/opt/airflow/dags/data/sm_f/total_cost_price_to_load.csv", index=False)


## Load data into ClickHouse
def load_cost_price_clickhouse():

    # ClickHouse server connection
    client = client_connect()

    client.command('''
        CREATE TABLE IF NOT EXISTS raw_proxima.cost_price(
            order_id String,
            type_return Nullable(String),   
            cost_price Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY order_id;
    ''')    

    
    # ✅ Read from saved files
    if os.path.exists("/opt/airflow/dags/data/sm_f/total_cost_price_to_load.csv"):
        total_cost_price = pd.read_csv("/opt/airflow/dags/data/sm_f/total_cost_price_to_load.csv").astype(str)
        if not total_cost_price.empty:
            client.insert_df('raw_proxima.cost_price', total_cost_price)


