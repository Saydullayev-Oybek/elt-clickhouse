from airflow import DAG
from pendulum import timezone
from datetime import datetime, timedelta
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
from transform.order_item import transform_order_item
from transform.order_transaction import transform_order_transaction
from transform.returns import transform_return
from transform.return_item import transform_return_item
from transform.return_transaction import transform_return_transaction
from transform.purchase import transform_purchase
from transform.purchase_item import transform_purchase_item
from transform.purchase_transaction import transform_purchase_transaction
from transform.return_supplier import transform_return_supplier
from transform.return_supplier_item import transform_return_supplier_item
from transform.ret_supplier_tran import transform_ret_supplier_transaction
from transform.writeoff import transform_writeoff
from transform.writeoff_item import transform_writeoff_item
from transform.writeoff_transaction import transform_writeoff_transaction
from transform.bank_operation import transform_bank_operation
from transform.bank_opr_tran import transform_bank_transaction
from transform.cash_operation import transform_cash_operation
from transform.cash_opr_tran import transform_cash_transaction


uz_tz = timezone("Asia/Tashkent")

default_args = {
    'start_date': datetime(2025, 1, 1, 23, 0, tzinfo=uz_tz),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        "transform", 
        default_args=default_args, 
        schedule_interval="50 23 * * *", 
        catchup=False
    ) as dag:
    
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
    transform_order_item = PythonOperator(
        task_id='transform_order_item',    
        python_callable=transform_order_item
    )
    transform_order_transaction = PythonOperator(
        task_id='transform_order_transaction',    
        python_callable=transform_order_transaction
    )
    transform_return= PythonOperator(
        task_id='transform_return',    
        python_callable=transform_return
    )
    transform_return_item= PythonOperator(
        task_id='transform_return_item',    
        python_callable=transform_return_item
    )
    transform_return_transaction = PythonOperator(
        task_id='transform_return_transaction',    
        python_callable=transform_return_transaction
    )
    transform_purchase = PythonOperator(
        task_id='transform_purchase',    
        python_callable=transform_purchase
    )
    transform_purchase_item = PythonOperator(
        task_id='transform_purchase_item',    
        python_callable=transform_purchase_item
    )
    transform_purchase_transaction = PythonOperator(
        task_id='transform_purchase_transaction',    
        python_callable=transform_purchase_transaction
    )
    transform_return_supplier = PythonOperator(
        task_id='transform_return_supplier',    
        python_callable=transform_return_supplier
    )
    transform_return_supplier_item = PythonOperator(
        task_id='transform_return_supplier_item',    
        python_callable=transform_return_supplier_item
    )
    transform_ret_supplier_transaction = PythonOperator(
        task_id='transform_ret_supplier_transaction',    
        python_callable=transform_ret_supplier_transaction
    )
    transform_bank_operation = PythonOperator(
        task_id='transform_bank_operation',    
        python_callable=transform_bank_operation
    )
    transform_bank_transaction = PythonOperator(
        task_id='transform_bank_transaction',    
        python_callable=transform_bank_transaction
    )
    transform_cash_operation = PythonOperator(
        task_id='transform_cash_operation',    
        python_callable=transform_cash_operation
    )
    transform_cash_transaction = PythonOperator(
        task_id='transform_cash_transaction',    
        python_callable=transform_cash_transaction
    )
    transform_writeoff = PythonOperator(
        task_id='transform_writeoff',    
        python_callable=transform_writeoff
    )
    transform_writeoff_item = PythonOperator(
        task_id='transform_writeoff_item',    
        python_callable=transform_writeoff_item
    )
    transform_writeoff_transaction = PythonOperator(
        task_id='transform_writeoff_transaction',    
        python_callable=transform_writeoff_transaction
    )




    [
        transform_company >> 
        transform_partner >>
        transform_product >> 
        transform_employee >> 
        transform_warehouse >> 
        transform_exchange_rate >>
        transform_account >> 
        transform_cost_center >> 
        transform_product_category >> 
        transform_customer_group >> 
        transform_order >> transform_order_item >> transform_order_transaction >> 
        transform_return >> transform_return_item >> transform_return_transaction >>
        transform_return_supplier >> transform_return_supplier_item >> transform_ret_supplier_transaction >>
        transform_purchase >> transform_purchase_item >> transform_purchase_transaction >>
        transform_writeoff >> transform_writeoff_item >> transform_writeoff_transaction >>
        transform_bank_operation >> transform_bank_transaction >>
        transform_cash_operation >> transform_cash_transaction 
    ]

