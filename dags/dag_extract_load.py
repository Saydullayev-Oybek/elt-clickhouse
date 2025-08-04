from airflow import DAG
from airflow.operators.python import PythonOperator
from extract_load.inventory import download_inventory, load_inventory_clickhouse
from extract_load.price_type import download_price_type, load_price_type_clickhouse 
from extract_load.product_group import download_product_group, load_product_group_clickhouse
from extract_load.person_group import download_person_group, load_person_group_clickhouse
from extract_load.legal_entity import download_legal_entity, load_legal_entity_clickhouse
from extract_load.natural_persons import download_natural_person, load_natural_person_clickhouse
from extract_load.company import download_company, load_company_clickhouse
from extract_load.warehouse import download_warehouse, load_warehouse_clickhouse
from extract_load.accounts import download_accounts, load_accounts_clickhouse
from extract_load.exchange_rates import download_currency, load_currency_clickhouse 
from extract_load.workspace import download_workspace, load_workspace_clickhouse
from extract_load.order import download_order, load_order_clickhouse
from extract_load.cost_price import download_cost_price, load_cost_price_clickhouse
from extract_load.returns import download_return, load_return_clickhouse
from datetime import datetime


default_args = {
'start_date': datetime.now(),
}

with DAG("extract_load", default_args=default_args, schedule_interval="@daily", catchup=False) as dag:
    fetch_inventory = PythonOperator(
        task_id='download_inventory',
        python_callable=download_inventory
    )
    load_inventory = PythonOperator(
        task_id='load_clickhouse',
        python_callable=load_inventory_clickhouse
    )
    fetch_price = PythonOperator(
        task_id='download_price_type',
        python_callable=download_price_type
    )
    load_price = PythonOperator(
        task_id='load_price_type_clickhouse',
        python_callable=load_price_type_clickhouse
    )
    fetch_person_group = PythonOperator(
        task_id='download_person_group',
        python_callable=download_person_group
    )
    load_person_group = PythonOperator(
        task_id='load_person_group_clickhouse',
        python_callable=load_person_group_clickhouse
    )
    fetch_legal_entity = PythonOperator(
        task_id='download_legal_entity',
        python_callable=download_legal_entity
    )
    load_legal_entity = PythonOperator(
        task_id='load_legal_entity_clickhouse', 
        python_callable=load_legal_entity_clickhouse
    )
    fetch_natural_person = PythonOperator(
        task_id='download_natural_person',  
        python_callable=download_natural_person
    )
    load_natural_person = PythonOperator(
        task_id='load_natural_person_clickhouse',
        python_callable=load_natural_person_clickhouse
    )
    fetch_workspace = PythonOperator(
        task_id='download_workspace',
        python_callable=download_workspace
    )
    load_workspace = PythonOperator(
        task_id='load_workspace',
        python_callable=load_workspace_clickhouse
    )
    fetch_order = PythonOperator(
        task_id='download_order',
        python_callable=download_order
    )
    load_order = PythonOperator(
        task_id='load_order',
        python_callable=load_order_clickhouse
    )
    fetch_return = PythonOperator(
        task_id='download_return',
        python_callable=download_return
    )
    load_return = PythonOperator(
        task_id='load_return',
        python_callable=load_return_clickhouse
    )
    fetch_product_group = PythonOperator(
        task_id='download_product_group',
        python_callable=download_product_group
    )
    load_product_group = PythonOperator(
        task_id='load_product_group_clickhouse',
        python_callable=load_product_group_clickhouse
    )

    # From excel files
    fetch_company = PythonOperator(
        task_id='download_company',
        python_callable=download_company
    )   
    load_company = PythonOperator(
        task_id='load_company_clickhouse',
        python_callable=load_company_clickhouse
    )
    fetch_warehouse = PythonOperator(
        task_id='download_warehouse',
        python_callable=download_warehouse
    )
    load_warehouse = PythonOperator(
        task_id='load_warehouse_clickhouse',
        python_callable=load_warehouse_clickhouse
    )
    fetch_accounts = PythonOperator(
        task_id='download_accounts',
        python_callable=download_accounts
    )
    load_accounts = PythonOperator(
        task_id='load_accounts_clickhouse',
        python_callable=load_accounts_clickhouse
    )
    fetch_currency = PythonOperator(
        task_id='download_currency',
        python_callable=download_currency
    )
    load_currency = PythonOperator(
        task_id='load_currency_clickhouse',
        python_callable=load_currency_clickhouse
    )
    fetch_cost_price = PythonOperator(
        task_id='download_cost_price',
        python_callable=download_cost_price
    )
    load_cost_price = PythonOperator(
        task_id='load_cost_price_clickhouse',
        python_callable=load_cost_price_clickhouse
    )


    [
        fetch_inventory >> load_inventory >>
        fetch_price >> load_price >>
        fetch_person_group >> load_person_group >> 
        fetch_legal_entity >> load_legal_entity >>
        fetch_natural_person >> load_natural_person >>
        fetch_workspace >> load_workspace >>
        fetch_order >> load_order >>
        fetch_return >> load_return >>
        fetch_product_group >> load_product_group >>
        fetch_company >> load_company >>
        fetch_warehouse >> load_warehouse >>
        fetch_accounts >> load_accounts >>
        fetch_currency >> load_currency >>
        fetch_cost_price >> load_cost_price
    ] 


