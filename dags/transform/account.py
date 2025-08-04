from utils import client_connect

def transform_account():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS `transformed_proxima`.account(
            account_id UInt32,
            account_name Nullable(String),
            account_type Nullable(String),
            type_currency Nullable(String),
            company_id Nullable(UInt32)
        )
        ENGINE = MergeTree
        ORDER BY account_id
    """
    client.command(create_table_query)

    
    insert_data_query = """
        INSERT INTO `transformed_proxima`.account 
        SELECT
            toUInt32OrNull(a.id) AS account_id,
            a.name AS account_name,
            a.account_type,
            a.type_currency,
            toUInt32OrNull((
                SELECT organization_id
                FROM raw_proxima.company 
                WHERE name = 'Proxima-Матрасы'
            )) as company_id
        FROM `raw_proxima`.account a
        LEFT JOIN `transformed_proxima`.account a2 ON toUInt32OrNull(a.id) = a2.account_id
        WHERE a2.account_id = 0
    """

    client.command(insert_data_query)