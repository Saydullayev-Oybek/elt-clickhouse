from utils import client_connect

def transform_exchange_rate():
    # ClickHouse server connection
    client = client_connect()

    # SQL query to transform company data
    create_table_query = """
        CREATE TABLE IF NOT EXISTS `transformed_proxima`.exchange_rate(
            date Date,
            from_currency Nullable(String),
            to_currency Nullable(String),
            rate Nullable(Float32)
        )
        ENGINE = MergeTree
        ORDER BY date
    """
    client.command(create_table_query)

    
    insert_data_query = """
        INSERT INTO `transformed_proxima`.exchange_rate
        SELECT
            toDate(parseDateTimeBestEffortOrNull(c.`date`)) AS date,
            c.`from_currency`,
            c.`to_currency`,
            toFloat32(c.rate) AS rate
        FROM `raw_proxima`.currency c 
        WHERE toDate(parseDateTimeBestEffortOrNull(c.`date`)) > (SELECT MAX(date) FROM `transformed_proxima`.exchange_rate)
    """

    client.command(insert_data_query)