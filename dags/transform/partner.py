from utils import client_connect

def transform_partner():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS transformed_proxima.partner(
            partner_id UInt32,
            partner_name Nullable(String),
            partner_type Nullable(String),
            country_name Nullable(String),
            company_id Nullable(UInt32),
            group_id Nullable(UInt32)
        )
        ENGINE = MergeTree
        ORDER BY partner_id
    """
    client.command(create_table_query)

    
    insert_data_query = """
        INSERT INTO `transformed_proxima`.partner 
        SELECT 
            toUInt32OrNull(l.person_id) as partner_id,
            l.name as partner_name,
            CASE
                WHEN l.is_client = 'Y' and l.is_supplier = 'Y' THEN 'client_supplier'
                WHEN l.is_client = 'Y' and l.is_supplier = 'N' THEN 'client'
                WHEN l.is_client = 'N' and l.is_supplier = 'Y' THEN 'supplier'
                ELSE ''
            END as partner_type,
            l.address country_name,
            toUInt32OrNull((
                SELECT organization_id
                FROM raw_proxima.company 
                WHERE name = 'Proxima-Матрасы'
            )) as company_id,
            toUInt32OrNull(l2.group_id) as group_id
        FROM raw_proxima.`legal_person` l 
        LEFT JOIN raw_proxima.`legal_person_group` l2 ON l.`person_id` = l2.`person_id` 
        LEFT JOIN transformed_proxima.partner p ON toUInt32OrNull(l.`person_id`) = p.`partner_id`
        WHERE p.partner_id = 0
    """

    client.command(insert_data_query)