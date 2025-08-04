from utils import client_connect

def transform_employee():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS `transformed_proxima`.employee(
            employee_id UInt32,
            employee_name Nullable(String),
            position Nullable(String),
            department Nullable(String),
            email Nullable(String),
            company_id Nullable(UInt32)
        )
        ENGINE = MergeTree
        ORDER BY employee_id
    """
    client.command(create_table_query)

    
    insert_data_query = """
        INSERT INTO `transformed_proxima`.employee
        SELECT 
            toUInt32OrNull(n.`person_id`) AS employee_id,
            concat_ws(' ', coalesce(n.`first_name`, ''), coalesce(n.`last_name`, ''), coalesce(n.middle_name, '')) AS employee_name,
            null AS position,
            w.room_name AS department,
            n.email AS email,
            toUInt32OrNull((
                SELECT organization_id
                FROM raw_proxima.company 
                WHERE name = 'Proxima-Матрасы'
            )) AS company_id
        FROM `raw_proxima`.`natural_person` n 
        LEFT JOIN `raw_proxima`.`natural_person_room` n2 ON n.person_id = n2.`person_id`
        LEFT JOIN `raw_proxima`.workspace w  ON w.`room_id` = n2.`room_id` 
        LEFT JOIN `transformed_proxima`.employee e ON toUInt32OrNull(n.person_id) = e.employee_id
        WHERE `is_client` = 'N' and `is_supplier` = 'N' and e.employee_id = 0
    """

    client.command(insert_data_query)