from utils import client_connect

def transform_return_supplier():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS `transformed_proxima`.return_supplier (
            return_id UInt32,
            return_date	Nullable(Date),
            currency_code Nullable(String),
            supplier_code Nullable(String),
            return_amount Nullable(Float32),
            status Nullable(String),
            note Nullable(String)
        )
            ENGINE = MergeTree
            ORDER BY return_id
    """ 
    client.command(create_table_query)

    insert_data_query = """
        INSERT INTO `transformed_proxima`.return_supplier
        SELECT 
            toUInt32(r.`return_id`) AS return_id,
            MAX(toDate(parseDateTimeBestEffortOrNull(r.return_time))) AS return_Date,
            MAX(r.`currency_code`) AS currency_code,
            MAX(r.`supplier_code`) AS supplier_code,
            SUM(
                if(isNaN(toFloat32(r2.quantity)), 0, toFloat32(r2.quantity)) * if(isNaN(toFloat32(r2.price )), 0, toFloat32(r2.price ))
            ) AS return_amount,
            MAX(r.status) AS status,
            MAX(r.note) AS note
        FROM `raw_proxima`.`return_supplier` r
        LEFT JOIN `raw_proxima`.`return_supplier_item` r2 ON r.`return_id`  = r2.`return_id`
        LEFT JOIN `transformed_proxima`.return_supplier t ON toUInt32(r.return_id) = t.return_id
        WHERE t.return_id = 0
        GROUP BY r.`return_id`
    """
    
    client.command(insert_data_query)

