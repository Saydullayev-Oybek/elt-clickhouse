import pandas as pd
from utils import client_connect

def transform_order_transaction():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS `transformed_proxima`.order_transaction(
            transaction_id UInt32,
            date Date,
            currency_code Nullable(String),
            account Nullable(String),
            amount Nullable(Float32),
            "kt/dt" Nullable(String),
            type Nullable(String),
            transaction_type DEFAULT('order')
        )
        ENGINE = MergeTree
        ORDER BY date
    """ 
    client.command(create_table_query)

    data_query = """
        SELECT 
            o.order_id,
            o.date,
            o.currency_code,
            o.total_amount,
            o.cost_amount,
            o.vat_amount,
            'order'
        FROM `transformed_proxima`.order o
        LEFT JOIN `transformed_proxima`.order_transaction t ON o.order_id = t.transaction_id
        WHERE t.transaction_id = 0
    """
    
    orders = client.query_df(data_query)

    def expand_row(row):
            return pd.DataFrame([
                {   
                    'transaction_id': row['order_id'],
                    'date': row['date'],
                    'currency_code': row['currency_code'],
                    'account': '2900',
                    'amount': row['cost_amount'],
                    'kt/dt': 'kt',
                    'type': 'cost_amount'
                },
                {
                    'transaction_id': row['order_id'],
                    'date': row['date'],
                    'currency_code': row['currency_code'],
                    'account': '4000',
                    'amount': row['total_amount'],
                    'kt/dt': 'dt',
                    'type': 'sale_amount'
                },
                {
                    'transaction_id': row['order_id'],
                    'date': row['date'],
                    'currency_code': row['currency_code'],
                    'account': '6400',
                    'amount': row['vat_amount'],
                    'kt/dt': 'kt',
                    'type': 'sale_vat_amount'
                },
                {
                    'transaction_id': row['order_id'],
                    'date': row['date'],
                    'currency_code': row['currency_code'],
                    'account': '9000',
                    'amount': row['total_amount'] - row['vat_amount'],
                    'kt/dt': 'kt',
                    'type': "'sale_amount' - 'sale_vat_amount'"
                },
                {
                    'transaction_id': row['order_id'],
                    'date': row['date'],
                    'currency_code': row['currency_code'],
                    'account': '9100',
                    'amount': row['cost_amount'],
                    'kt/dt': 'dt',
                    'type': 'cost_amount'
                }
            ])

    if not orders.empty:
        
        expanded_df = pd.concat([expand_row(row) for _, row in orders.iterrows()], ignore_index=True)

        # Insert df to clickhouse
        client.insert_df('transformed_proxima.order_transaction', expanded_df)