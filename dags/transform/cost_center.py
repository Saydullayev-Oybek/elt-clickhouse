from utils import client_connect

def transform_cost_center():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS `transformed_proxima`.cost_center(
            cost_center_code String,
            cost_center_name Nullable(String),
            department Nullable(String),
            company_id Nullable(UInt32)
        )
        ENGINE = MergeTree
        ORDER BY cost_center_code
    """
    client.command(create_table_query)

    
    insert_data_query = """
        INSERT INTO `transformed_proxima`.cost_center 
        SELECT
            p.code AS cost_center_code,
            p.name AS cost_center_name,
            null AS department,
            toUInt32OrNull((
                SELECT organization_id
                FROM raw_proxima.company
                WHERE name = 'Proxima-Матрасы'
            )) AS company_id
        FROM `raw_proxima`.price_type p
        LEFT JOIN `transformed_proxima`.cost_center p2 ON p.name = p2.cost_center_name
        WHERE p2.cost_center_name is null
    """

    client.command(insert_data_query)