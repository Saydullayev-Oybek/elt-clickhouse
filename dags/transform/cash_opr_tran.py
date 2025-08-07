import pandas as pd
from utils import client_connect

def transform_cash_transaction():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS `transformed_proxima`.cash_opr_tran(
            transaction_id UInt32,
            date Date,
            currency_code Nullable(String),
            account Nullable(String),
            amount Nullable(Float32),
            "kt/dt" Nullable(String),
            type Nullable(String),
            transaction_type DEFAULT('cash_operation')
        )
        ENGINE = MergeTree
        ORDER BY date
    """ 
    client.command(create_table_query)

    data_query = """
        SELECT 
            b.`operation_id`,
            b.`operation_date`,
            b.`currency_code`,
            b.account,
            b.amount,
            b.`cashflow_kind`
        FROM `transformed_proxima`.`cash_operation` b  
        LEFT JOIN `transformed_proxima`.cash_opr_tran t ON b.`operation_id` = t.transaction_id
        WHERE t.transaction_id = 0
    """

    def expand_row_kt(row):
            return pd.DataFrame([
                {   
                    'transaction_id': row['operation_id'],
                    'date': row['operation_date'],
                    'currency_code': row['currency_code'],
                    'account': row['account'],
                    'amount': row['amount'],
                    'kt/dt': 'kt',
                    'type': 'amount',
                },
                {
                    'transaction_id': row['operation_id'],
                    'date': row['operation_date'],
                    'currency_code': row['currency_code'],
                    'account': '5000',
                    'amount': row['amount'],
                    'kt/dt': 'dt',
                    'type': 'amount',
                }
            ])
    def expand_row_dt(row):
        return pd.DataFrame([
            {   
                'transaction_id': row['operation_id'],
                'date': row['operation_date'],
                'currency_code': row['currency_code'],
                'account': row['account'],
                'amount': row['amount'],
                'kt/dt': 'dt',
                'type': 'amount',
            },
            {
                'transaction_id': row['operation_id'],
                'date': row['operation_date'],
                'currency_code': row['currency_code'],
                'account': '5000',
                'amount': row['amount'],
                'kt/dt': 'kt',
                'type': 'amount',
            }
        ])
        
    cash_opr = client.query_df(data_query)
    if not cash_opr.empty:
        cash_opr_kt = cash_opr[cash_opr['cashflow_kind'] == 'I']
        cash_opr_dt = cash_opr[cash_opr['cashflow_kind'] == 'E']

        if not cash_opr_kt.empty:    
            expanded_df_kt = pd.concat([expand_row_kt(row) for _, row in cash_opr_kt.iterrows()], ignore_index=True)
        if not cash_opr_dt.empty:
            expanded_df_dt = pd.concat([expand_row_dt(row) for _, row in cash_opr_dt.iterrows()], ignore_index=True)

        if not cash_opr_kt.empty and not cash_opr_dt.empty:
            df = pd.concat([expanded_df_dt, expanded_df_kt])

            # Insert df to clickhouse
            client.insert_df('transformed_proxima.cash_opr_tran', df)