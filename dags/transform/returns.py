from utils import client_connect

def transform_return():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS `transformed_proxima`.return(
            return_id UInt32,
            date Nullable(Date),
            sales_manager_code Nullable(UInt32),
            client_id Nullable(UInt32),
            currency_code Nullable(String),
            status Nullable(String),
            total_amount Nullable(Float32),
            cost_amount Nullable(Float32),
            vat_amount Nullable(Float32)
        )
        ENGINE = MergeTree
        ORDER BY return_id
    """
    client.command(create_table_query)

    insert_data_query = """
        INSERT INTO `transformed_proxima`.`return` 
        SELECT 
            toUInt32(r.deal_id) AS return_id,
            toDate(parseDateTimeBestEffortOrNull(r.delivery_date)) AS date,
            r.sales_manager_code AS sales_manager_code,
            toUInt32(r.person_id) AS client_id,
            r.currency_code AS currency_code,
            r.status AS status,
            toFloat32(r.total_amount) * -1 AS total_amount,
            c.`cost_amount` AS cost_amount, 
            v.`cost_amount` AS vat_amount
        FROM `raw_proxima`.`return` r
        LEFT JOIN (
            SELECT 
                order_id,
                SUM(if(isNaN(toFloat32(cost_price)), 0, toFloat32(cost_price))) * -1 AS cost_amount
            FROM `raw_proxima`.`cost_price`
            GROUP BY order_id
        ) c ON r.`deal_id` = c.order_id
        LEFT JOIN (
            SELECT 
                deal_id AS return_id,
                SUM(if(isNaN(toFloat32(`vat_amount` )), 0, toFloat32(`vat_amount` ))) * -1 AS cost_amount
            FROM `raw_proxima`.`return_item` 
            GROUP BY deal_id
        ) v ON v.`return_id`  = r.`deal_id`
        LEFT JOIN `transformed_proxima`.return t ON toUInt32(r.deal_id) = t.`return_id` 
        WHERE t.`return_id`  = 0 AND r.status = 'A'
    """
    
    client.command(insert_data_query)

