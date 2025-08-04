from utils import client_connect

def transform_order():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS `transformed_proxima`.order(
            order_id UInt32,
            filial_id Nullable(UInt32),
            date Nullable(Date),
            sales_manager_id Nullable(UInt32),
            person_id Nullable(UInt32),
            currency_code Nullable(String),
            status Nullable(String),
            total_amount Nullable(Float32),
            cost_amount Nullable(Float32),
            vat_amount Nullable(Float32)
        )
        ENGINE = MergeTree
        ORDER BY order_id
    """ 
    client.command(create_table_query)

    insert_data_query = """
        INSERT INTO `transformed_proxima`.order 
        SELECT 
            toUInt32(o.deal_id) AS order_id,
            toUInt32(o.filial_id) AS filial_id,
            toDate(parseDateTimeBestEffortOrNull(o.delivery_date)) AS date,
            toUInt32(o.sales_manager_id) AS sales_manager_id,
            toUInt32(o.person_id) AS person_id,
            o.currency_code AS currency_code,
            o.status AS status,
            toFloat32(o.total_amount) AS total_amount,
            c.`cost_amount` AS cost_amount, 
            v.`cost_amount` AS vat_amount
        FROM `raw_proxima`.`order` o
        LEFT JOIN (
            SELECT 
                order_id,
                SUM(if(isNaN(toFloat32(cost_price)), 0, toFloat32(cost_price))) AS cost_amount
            FROM `raw_proxima`.`cost_price`
            GROUP BY order_id
        ) c ON o.`deal_id` = c.order_id
        LEFT JOIN (
            SELECT 
                deal_id AS order_id,
                SUM(if(isNaN(toFloat32(`vat_amount` )), 0, toFloat32(`vat_amount` ))) AS cost_amount
            FROM `raw_proxima`.`order_item` 
            GROUP BY deal_id
        ) v ON v.`order_id`  = o.`deal_id`
        LEFT JOIN `transformed_proxima`.order t ON toUInt32(o.deal_id) = t.order_id
        WHERE t.order_id = 0 AND o.status = 'A'
    """
    
    client.command(insert_data_query)

