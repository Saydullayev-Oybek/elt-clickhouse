from utils import client_connect

def transform_return_item():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS `transformed_proxima`.return_item(
            product_unit_id UInt32,
            return_id UInt32,
            product_code Nullable(UInt32),
            return_quant Nullable(Float32),
            product_price Nullable(Float32),
            vat_amount Nullable(Float32),
            sold_amount Nullable(Float32),
            inventory_kind Nullable(String),
            on_balance Nullable(String)
        )
        ENGINE = MergeTree
        ORDER BY return_id
    """ 
    client.command(create_table_query)

    insert_data_query = """
        INSERT INTO `transformed_proxima`.`return_item`
        SELECT 
            toUInt32(o.product_unit_id) AS product_unit_id,
            toUInt32(o.deal_id) AS return_id,
            toUInt32(o.product_code) AS product_code,
            toFloat32(o.return_quant) AS return_quant,
            toFloat32(o.product_price) AS product_price,
            toFloat32(o.vat_amount) AS vat_amount,
            toFloat32(o.sold_amount) AS sold_amount,
            o.inventory_kind,
            o.on_balance
        FROM `raw_proxima`.`return_item` o  
        LEFT JOIN `transformed_proxima`.`return_item` r ON toUInt32(o.deal_id) = r.`return_id` 
        WHERE r.`return_id` = 0
    """
    
    client.command(insert_data_query)

