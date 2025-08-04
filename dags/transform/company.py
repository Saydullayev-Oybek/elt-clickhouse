from utils import client_connect

def transform_company():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS transformed_proxima.company(
            company_id UInt32,
            inn Nullable(String),
            company_name Nullable(String),
            created_date Nullable(DateTime)
        )
        ENGINE = MergeTree
        ORDER BY company_id
    """
    client.command(create_table_query)

    insert_data_query = """
        INSERT INTO `transformed_proxima`.company 
        SELECT 
            toUInt32(organization_id),
            inn,
            name,
            parseDateTimeBestEffortOrNull(date)
        FROM raw_proxima.company rw_c
        LEFT JOIN transformed_proxima.company tr_c
            ON toUInt32(organization_id) = tr_c.company_id
        WHERE tr_c.company_id = 0
    """
    
    client.command(insert_data_query)