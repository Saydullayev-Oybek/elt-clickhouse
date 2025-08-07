import pandas as pd
from utils import client_connect

def transform_writeoff_transaction():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS `transformed_proxima`.writeoff_transaction(
            transaction_id UInt32,
            date Date,
            currency_code Nullable(String),
            account Nullable(String),
            amount Nullable(Float32),
            "kt/dt" Nullable(String),
            type Nullable(String),
            transaction_type DEFAULT('writeoff')
        )
        ENGINE = MergeTree
        ORDER BY date
    """ 
    client.command(create_table_query)

    data_query = """
        SELECT 
            b.`writeoff_id`,
            b.`writeoff_date`,
            b.`currency_code`,
            b.`c_amount`,
        FROM `transformed_proxima`.`writeoff` b  
        LEFT JOIN `transformed_proxima`.writeoff_transaction t ON b.`writeoff_id` = t.transaction_id
        WHERE t.transaction_id = 0
    """
    
    writeoff = client.query_df(data_query)

    def expand_row(row):
            return pd.DataFrame([
                {   
                    'transaction_id': row['writeoff_id'],
                    'date': row['writeoff_date'],
                    'currency_code': row['currency_code'],
                    'account': '1600',
                    'amount': row['c_amount'],
                    'kt/dt': 'dt',
                    'type': 'c_amount'
                },
                {
                    'transaction_id': row['writeoff_id'],
                    'date': row['writeoff_date'],
                    'currency_code': row['currency_code'],
                    'account': '2900',
                    'amount': row['c_amount'],
                    'kt/dt': 'kt',
                    'type': 'c_amount'
                }
            ])

    if not writeoff.empty:
        
        expanded_df = pd.concat([expand_row(row) for _, row in writeoff.iterrows()], ignore_index=True)

        # Insert df to clickhouse
        client.insert_df('transformed_proxima.writeoff_transaction', expanded_df)