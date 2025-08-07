from utils import client_connect

def transform_writeoff_item():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS `transformed_proxima`.writeoff_item (
            writeoff_item_id UInt32,
            writeoff_id	UInt32,
            product_code Nullable(String),
            quantity Nullable(Float32),
            inventory_kind Nullable(String)
        )
            ENGINE = MergeTree
            ORDER BY writeoff_id
    """ 
    client.command(create_table_query)

    insert_data_query = """
        INSERT INTO `transformed_proxima`.writeoff_item
        SELECT 
            toUInt32(b.`writeoff_item_id`),
            b.writeoff_id,
            b.product_code,
            b.quantity,
            b.inventory_kind
        FROM `raw_proxima`.writeoff_item b
        LEFT JOIN `transformed_proxima`.writeoff_item t ON toUInt32(b.`writeoff_id`) = t.writeoff_id
        WHERE t.writeoff_id = 0
    """
    
    client.command(insert_data_query)

