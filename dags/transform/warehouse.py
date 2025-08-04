from utils import client_connect

def transform_warehouse():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS `transformed_proxima`.warehouse(
            warehouse_id UInt32,
            warehouse_name Nullable(String),
            location Nullable(String),
            company_id Nullable(UInt32)
        )
        ENGINE = MergeTree
        ORDER BY warehouse_id
    """
    client.command(create_table_query)

    
    insert_data_query = """
        INSERT INTO `transformed_proxima`.warehouse 
        SELECT
            toUInt32OrNull(w.`warehouse_id`) AS warehouse_id,
            w.name AS warehouse_name,
            null AS location,
            toUInt32OrNull((
                SELECT organization_id
                FROM raw_proxima.company 
                WHERE name = 'Proxima-Матрасы'
            )) AS company_id
        FROM `raw_proxima`.warehouse w 
        LEFT JOIN `transformed_proxima`.warehouse w2 ON toUInt32OrNull(w.`warehouse_id`)  = w2.`warehouse_id`
        WHERE w2.`warehouse_id` = 0
    """

    client.command(insert_data_query)