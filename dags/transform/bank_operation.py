from utils import client_connect

def transform_bank_operation():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS `transformed_proxima`.bank_operation (
            operation_id UInt32,
            operation_date Nullable(Date),
            bank_account_code Nullable(String),
            cashflow_kind Nullable(String),
            account	Nullable(String),
            currency_code Nullable(String),
            amount Nullable(Float32),
            note Nullable(String)
        )
            ENGINE = MergeTree
            ORDER BY operation_id
    """ 
    client.command(create_table_query)

    insert_data_query = """
        INSERT INTO `transformed_proxima`.bank_operation
        SELECT 
            toUInt32(b.`operation_id`),
            toDate(parseDateTimeBestEffortOrNull(b.`operation_date`)),
            b.bank_account_code,
            b.cashflow_kind,
            b.`corr_coa_code`,
            b.currency_code,
            b.amount,
            b.note 
        FROM `raw_proxima`.bank_operation b
        LEFT JOIN `transformed_proxima`.bank_operation t ON toUInt32(b.`operation_id`) = t.operation_id
        WHERE t.operation_id = 0
    """
    
    client.command(insert_data_query)

