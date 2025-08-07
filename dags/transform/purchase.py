from utils import client_connect

def transform_purchase():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS `transformed_proxima`.purchase (
            purchase_id	UInt32,
            company_id UInt32,
            purchase_date Nullable(Date),
            supplier_code Nullable(String),
            currency_code Nullable(String),
            note Nullable(String),
            purchase_amount	Nullable(Float32)
        )
            ENGINE = MergeTree
            ORDER BY purchase_id
    """ 
    client.command(create_table_query)

    insert_data_query = """
        INSERT INTO `transformed_proxima`.purchase 
        SELECT 
            MAX(toUInt32(p.`purchase_id`)) AS purchase_id,
            toUInt32(6091241) AS company_id,
            MAX(toDate(parseDateTimeBestEffortOrNull(p.`purchase_time`))) AS purchase_time,
            MAX(p.`supplier_code`) AS supplier_code,
            MAX(p.`currency_code`) AS currency_code,
            MAX(p.`note`) AS note,
            SUM(
                if(isNaN(toFloat32(p2.quantity)), 0, toFloat32(p2.quantity)) * if(isNaN(toFloat32(p2.price )), 0, toFloat32(p2.price ))
            ) AS purchase_amount
        FROM `raw_proxima`.`purchase` p
        LEFT JOIN `raw_proxima`.`purchase_item` p2 ON p.`purchase_id` = p2.`purchase_id`
        LEFT JOIN `transformed_proxima`.purchase t ON toUInt32(p.`purchase_id`) = t.`purchase_id`
        WHERE t.`purchase_id` = 0 
        GROUP BY p.`purchase_id`
    """
    
    client.command(insert_data_query)

