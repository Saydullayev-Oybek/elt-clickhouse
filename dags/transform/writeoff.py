from utils import client_connect

def transform_writeoff():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS `transformed_proxima`.writeoff (
            writeoff_id	UInt32,
            writeoff_date Nullable(Date),
            currency_code Nullable(String),
            c_amount Nullable(Float32),
            c_amount_base Nullable(Float32),
            status Nullable(String),
            note Nullable(String)
        )
            ENGINE = MergeTree
            ORDER BY writeoff_id
    """ 
    client.command(create_table_query)

    insert_data_query = """
        INSERT INTO `transformed_proxima`.writeoff
        SELECT 
            toUInt32(b.`writeoff_id`),
            toDate(parseDateTimeBestEffortOrNull(b.`writeoff_date`)),
            b.currency_code,
            b.c_amount,
            b.`c_amount_base`,
            b.status,
            b.note 
        FROM `raw_proxima`.writeoff b
        LEFT JOIN `transformed_proxima`.writeoff t ON toUInt32(b.`writeoff_id`) = t.writeoff_id
        WHERE t.writeoff_id = 0
    """
    
    client.command(insert_data_query)

