from utils import client_connect

def transform_product():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS transformed_proxima.product(
            product_id UInt32,
            product_name Nullable(String),
            product_type Nullable(String),
            unit Nullable(String),
            category_id Nullable(UInt32)
        )
        ENGINE = MergeTree
        ORDER BY product_id
    """
    client.command(create_table_query)

    
    insert_data_query = """
        INSERT INTO `transformed_proxima`.product 
        SELECT 
            i.product_id,
            i.name,
            i.`inventory_kind`,
            null,
            i2.`group_id`
        FROM `raw_proxima`.inventory i 
        LEFT JOIN `raw_proxima`.`inventory_group` i2 ON i2.product_id = i.`product_id`
        LEFT JOIN `transformed_proxima`.product p ON p.`product_id` = toUInt32OrNull(i.`product_id`)
        WHERE p.`product_id`  = 0
    """

    client.command(insert_data_query)