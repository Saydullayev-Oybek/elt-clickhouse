from utils import client_connect

def transform_purchase_item():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS `transformed_proxima`.purchase_item (
            purchase_item_id UInt32,
            purchase_id	UInt32,
            product_code Nullable(String),
            purchase_quant Nullable(Float32),
            purchase_price Nullable(Float32),
            vat_amount Nullable(Float32),
            inventory_kind Nullable(String),
            on_balance Nullable(String)
        )
            ENGINE = MergeTree
            ORDER BY purchase_id
    """ 
    client.command(create_table_query)

    insert_data_query = """
        INSERT INTO `transformed_proxima`.purchase_item
        SELECT 
            toUInt32(p.`purchase_item_id`) AS purchase_item_id,
            toUInt32(p.`purchase_id`) AS purchase_id,
            p.`product_code`,
            toFloat32(p.quantity) AS purchas_quant,
            toFloat32(p.price) AS purchase_price,
            toFloat32(p.`vat_amount`) AS vat_amount,
            p.`inventory_kind`,
            p.`on_balance`
        FROM `raw_proxima`.`purchase_item` p 
        LEFT JOIN `transformed_proxima`.purchase_item t ON toUInt32(p.`purchase_id`) = t.`purchase_id`
        WHERE t.`purchase_id` = 0
    """
    
    client.command(insert_data_query)

