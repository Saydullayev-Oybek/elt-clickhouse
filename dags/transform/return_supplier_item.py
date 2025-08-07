from utils import client_connect

def transform_return_supplier_item():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS `transformed_proxima`.return_supllier_item (
            return_item_id	UInt32,
            return_id UInt32,
            product_code Nullable(String),
            return_quant Nullable(Float32),
            return_price Nullable(Float32),
            vat_amount Nullable(Float32),
            inventory_kind Nullable(String),
            on_balance Nullable(String)
        )
            ENGINE = MergeTree
            ORDER BY return_id
    """ 
    client.command(create_table_query)

    insert_data_query = """
        INSERT INTO `transformed_proxima`.return_supllier_item
        SELECT 
            toUInt32(r.`return_item_id`) AS return_item_id,
            toUInt32(r.`return_id`) AS return_id,
            r.`product_code`,
            toFloat32(r.quantity) AS return_quant,
            toFloat32(r.price) AS return_price,
            toFloat32(r.`vat_amount`) AS vat_amount,
            r.`inventory_kind`,
            r.`on_balance`
        FROM `raw_proxima`.`return_supplier_item` r
        LEFT JOIN `transformed_proxima`.return_supllier_item t ON toUInt32(r.return_id) = t.return_id
        WHERE t.return_id = 0
    """
    
    client.command(insert_data_query)

