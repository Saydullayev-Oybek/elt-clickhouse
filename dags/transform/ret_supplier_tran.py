import pandas as pd
from utils import client_connect

def transform_ret_supplier_transaction():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS `transformed_proxima`.ret_supplier_tran(
            transaction_id UInt32,
            date Date,
            currency_code Nullable(String),
            account Nullable(String),
            amount Nullable(Float32),
            "kt/dt" Nullable(String),
            type Nullable(String),
            transaction_type DEFAULT('return_supplier')
        )
        ENGINE = MergeTree
        ORDER BY date
    """ 
    client.command(create_table_query)

    data_query = """
        SELECT 
            b.`return_id`,
            b.`return_date`,
            b.`currency_code`,
            b.`return_amount`,
        FROM `transformed_proxima`.`return_supplier` b  
        LEFT JOIN `transformed_proxima`.ret_supplier_tran t ON b.`return_id` = t.transaction_id
        WHERE t.transaction_id = 0
    """
    
    return_supp = client.query_df(data_query)

    def expand_row(row):
            return pd.DataFrame([
                {   
                    'transaction_id': row['return_id'],
                    'date': row['return_date'],
                    'currency_code': row['currency_code'],
                    'account': '2900',
                    'amount': row['return_amount'],
                    'kt/dt': 'kt',
                    'type': 'return_amount'
                },
                {
                    'transaction_id': row['return_id'],
                    'date': row['return_date'],
                    'currency_code': row['currency_code'],
                    'account': '4300',
                    'amount': row['return_amount'],
                    'kt/dt': 'dt',
                    'type': 'return_amount'
                },
                {
                    'transaction_id': row['return_id'],
                    'date': row['return_date'],
                    'currency_code': row['currency_code'],
                    'account': '9400',
                    'amount': row['return_amount'],
                    'kt/dt': 'kt',
                    'type': 'return_amount'
                },
                {
                    'transaction_id': row['return_id'],
                    'date': row['return_date'],
                    'currency_code': row['currency_code'],
                    'account': '9400',
                    'amount': row['return_amount'],
                    'kt/dt': 'dt',
                    'type': 'return_amount'
                }
            ])

    if not return_supp.empty:
        
        expanded_df = pd.concat([expand_row(row) for _, row in return_supp.iterrows()], ignore_index=True)

        # Insert df to clickhouse
        client.insert_df('transformed_proxima.ret_supplier_tran', expanded_df)