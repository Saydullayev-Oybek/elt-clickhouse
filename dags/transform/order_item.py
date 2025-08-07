from utils import client_connect

def transform_order_item():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS `transformed_proxima`.order_item(
            product_unit_id UInt32,
            order_id UInt32,
            product_id Nullable(UInt32),
            order_quant Nullable(Float32),
            return_quant Nullable(Float32),
            product_price Nullable(Float32),
            vat_amount Nullable(Float32),
            sold_quant Nullable(Float32),
            sold_amount Nullable(Float32),
            inventory_kind Nullable(String),
            on_balance Nullable(String)
        )
        ENGINE = MergeTree
        ORDER BY order_id
    """ 
    client.command(create_table_query)

    insert_data_query = """
        INSERT INTO `transformed_proxima`.`order_item`
          SELECT 
            toUInt32(o.product_unit_id) AS product_unit_id,
            toUInt32(o.deal_id) AS order_id,
            toUInt32(o.product_id) AS product_id,
            toFloat32(o.order_quant) AS order_quant,
            toFloat32(o.return_quant) AS return_quant,
            toFloat32(o.product_price) AS product_price,
            toFloat32(o.vat_amount) AS vat_amount,
            toFloat32(o.sold_quant) AS sold_quant,
            toFloat32(o.sold_amount) AS sold_amount,
            o.inventory_kind,
            o.on_balance
        FROM `raw_proxima`.`order_item` o  
        LEFT JOIN `transformed_proxima`.`order_item` t ON toUInt32(o.deal_id) = t.order_id
        WHERE t.order_id = 0
    """
    
    client.command(insert_data_query)

