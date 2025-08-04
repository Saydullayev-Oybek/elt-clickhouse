from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from transform.company import transform_company
from transform.partner import transform_partner
from transform.products import transform_product
from transform.employee import transform_employee
from transform.warehouse import transform_warehouse
from transform.exchange_rate import transform_exchange_rate
from transform.account import transform_account
from transform.cost_center import transform_cost_center
from transform.product_category import transform_product_category
from transform.customer_group import transform_customer_group
from transform.order import transform_order
from transform.order_transaction import transform_transaction
from transform.returns import transform_return
from transform.return_transaction import transform_return_transaction

default_args = {
'start_date': datetime.now(),
}

with DAG("transform", default_args=default_args, schedule_interval="@daily", catchup=False) as dag:
    transform_company = PythonOperator(
        task_id='transform_company',
        python_callable=transform_company
    )
    transform_partner = PythonOperator(
        task_id='transform_partner',
        python_callable=transform_partner
    )
    transform_product = PythonOperator(
        task_id='transform_product',    
        python_callable=transform_product
    )
    transform_employee = PythonOperator(
        task_id='transform_employee',    
        python_callable=transform_employee
    )
    transform_warehouse = PythonOperator(
        task_id='transform_warehouse',    
        python_callable=transform_warehouse
    )
    transform_exchange_rate = PythonOperator(
        task_id='transform_exchange_rate',    
        python_callable=transform_exchange_rate
    )
    transform_account = PythonOperator(
        task_id='transform_account',    
        python_callable=transform_account
    )
    transform_cost_center = PythonOperator(
        task_id='transform_cost_center',    
        python_callable=transform_cost_center
    )
    transform_product_category = PythonOperator(
        task_id='transform_product_category',    
        python_callable=transform_product_category
    )
    transform_customer_group = PythonOperator(
        task_id='transform_customer_group',    
        python_callable=transform_customer_group
    )
    transform_order = PythonOperator(
        task_id='transform_order',    
        python_callable=transform_order
    )
    transform_transaction = PythonOperator(
        task_id='transform_transaction',    
        python_callable=transform_transaction
    )
    transform_return= PythonOperator(
        task_id='transform_return',    
        python_callable=transform_return
    )
    transform_return_transaction = PythonOperator(
        task_id='transform_return_transaction',    
        python_callable=transform_return_transaction
    )
    


    [
        transform_company >> transform_partner >> transform_product >> 
        transform_employee >> transform_warehouse >> transform_exchange_rate >>
        transform_account >> transform_cost_center >> transform_product_category >>
        transform_customer_group >> transform_order >> transform_transaction >>
        transform_return >> transform_return_transaction
    ]

