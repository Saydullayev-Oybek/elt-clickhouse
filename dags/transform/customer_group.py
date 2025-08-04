from utils import client_connect

def transform_customer_group():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS `transformed_proxima`.customer_group(
            group_id UInt32,
            group_name Nullable(String),
            group_type Nullable(String)
        )
        ENGINE = MergeTree
        ORDER BY group_id
    """
    client.command(create_table_query)

    
    insert_data_query = """
        INSERT INTO `transformed_proxima`.`customer_group`  
        SELECT
            p.person_group_id AS group_id,
            p.name AS group_name,
            p2.name as group_type
        FROM `raw_proxima`.`person_group` p
        LEFT JOIN `raw_proxima`.`person_group_type` p2 ON p.person_group_id = p2.person_group_id
        LEFT JOIN `transformed_proxima`.`customer_group` p3 ON toUInt32OrNull(p.person_group_id) = p3.group_id 
        WHERE p3.group_id = 0
    """

    client.command(insert_data_query)