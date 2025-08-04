from utils import client_connect

def transform_product_category():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS `transformed_proxima`.product_category(
            category_id UInt32,
            category_name Nullable(String),
            category_type Nullable(String)
        )
        ENGINE = MergeTree
        ORDER BY category_id

    """
    client.command(create_table_query)

    
    insert_data_query = """
        INSERT INTO `transformed_proxima`.`product_category`  
        SELECT
            p.product_group_id AS category_id,
            p.name AS category_name,
            p2.name as category_type
        FROM `raw_proxima`.`product_group` p
        LEFT JOIN `raw_proxima`.`product_group_type` p2 ON p.product_group_id = p2.product_group_id
        LEFT JOIN `transformed_proxima`.`product_category` p3 ON toUInt32OrNull(p.product_group_id) = p3.category_id 
        WHERE p3.category_id = 0
    """

    client.command(insert_data_query)