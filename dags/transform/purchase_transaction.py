import pandas as pd
from utils import client_connect

def transform_purchase_transaction():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS `transformed_proxima`.purchase_transaction(
            transaction_id UInt32,
            date Date,
            currency_code Nullable(String),
            account Nullable(String),
            amount Nullable(Float32),
            "kt/dt" Nullable(String),
            type Nullable(String),
            transaction_type DEFAULT('purchase')
        )
        ENGINE = MergeTree
        ORDER BY date
    """ 
    client.command(create_table_query)

    data_query = """
        SELECT 
            b.`purchase_id`,
            b.`purchase_date`,
            b.`currency_code`,
            b.`purchase_amount`,
        FROM `transformed_proxima`.`purchase` b  
        LEFT JOIN `transformed_proxima`.purchase_transaction t ON b.`purchase_id` = t.transaction_id
        WHERE t.transaction_id = 0
    """
    
    purchase = client.query_df(data_query)

    def expand_row(row):
            return pd.DataFrame([
                {   
                    'transaction_id': row['purchase_id'],
                    'date': row['purchase_date'],
                    'currency_code': row['currency_code'],
                    'account': '2900',
                    'amount': row['purchase_amount'],
                    'kt/dt': 'dt',
                    'type': 'purchase_amount'
                },
                {
                    'transaction_id': row['purchase_id'],
                    'date': row['purchase_date'],
                    'currency_code': row['currency_code'],
                    'account': '6000',
                    'amount': row['purchase_amount'],
                    'kt/dt': 'kt',
                    'type': 'purchase_amount'
                }
            ])

    if not purchase.empty:
        
        expanded_df = pd.concat([expand_row(row) for _, row in purchase.iterrows()], ignore_index=True)

        # Insert df to clickhouse
        client.insert_df('transformed_proxima.purchase_transaction', expanded_df)