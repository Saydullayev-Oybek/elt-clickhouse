import pandas as pd
from utils import client_connect

def transform_return_transaction():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS `transformed_proxima`.return_transaction(
            transaction_id UInt32,
            date Date,
            currency_code Nullable(String),
            account Nullable(String),
            amount Nullable(Float32),
            "kt/dt" Nullable(String),
            type Nullable(String)
        )
        ENGINE = MergeTree
        ORDER BY date
    """ 
    client.command(create_table_query)

    data_query = """
        SELECT 
            r.return_id,
            r.date,
            r.currency_code,
            r.total_amount,
            r.cost_amount,
            r.vat_amount
        FROM `transformed_proxima`.return r
        LEFT JOIN `transformed_proxima`.return_transaction t ON r.return_id = t.transaction_id
        WHERE t.transaction_id = 0
    """
    
    returns = client.query_df(data_query)

    def expand_row(row):
            return pd.DataFrame([
                {   
                    'transaction_id': row['return_id'],
                    'date': row['date'],
                    'currency_code': row['currency_code'],
                    'account': '2900',
                    'amount': row['cost_amount'],
                    'kt/dt': 'dt',
                    'type': 'cost_amount'
                },
                {
                    'transaction_id': row['return_id'],
                    'date': row['date'],
                    'currency_code': row['currency_code'],
                    'account': '4400',
                    'amount': row['vat_amount'],
                    'kt/dt': 'dt',
                    'type': 'vat_amount'
                },
                {
                    'transaction_id': row['return_id'],
                    'date': row['date'],
                    'currency_code': row['currency_code'],
                    'account': '6300',
                    'amount': row['total_amount'],
                    'kt/dt': 'kt',
                    'type': 'sale_amount'
                },
                {
                    'transaction_id': row['return_id'],
                    'date': row['date'],
                    'currency_code': row['currency_code'],
                    'account': '9000',
                    'amount': row['total_amount'] - row['vat_amount'],
                    'kt/dt': 'dt',
                    'type': "'sale_amount' - 'vat_amount'"
                },
                {
                    'transaction_id': row['return_id'],
                    'date': row['date'],
                    'currency_code': row['currency_code'],
                    'account': '9100',
                    'amount': row['cost_amount'],
                    'kt/dt': 'kt',
                    'type': 'cost_amount'
                    
                }
            ])

    if not returns.empty:
        
        expanded_df = pd.concat([expand_row(row) for _, row in returns.iterrows()], ignore_index=True)

        # Insert df to clickhouse
        client.insert_df('transformed_proxima.return_transaction', expanded_df)