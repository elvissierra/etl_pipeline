import sqlite3
from etl.logger import setup_logger
logger = setup_logger(__name__)


def load_data(df, db_path, table_name, table_schema):
    logger.info("Opening db connection to %s", db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Construct CREATE TABLE SQL
    schema_parts = [f"{col} {dtype}" for col, dtype in table_schema.items()]
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join(schema_parts)}
    )"""
    cursor.execute(create_table_sql)
    logger.info("Inserting %d rows into %s", len(df), table_name)

    # Insert data
    columns = list(table_schema.keys())
    placeholders = ', '.join(['?'] * len(columns))
    insert_sql = f"""
        INSERT OR IGNORE INTO {table_name} (
            {', '.join(columns)}
        ) VALUES ({placeholders})
    """

    for _, row in df.iterrows():
        values = [row.get(col) for col in columns]
        cursor.execute(insert_sql, values)

    conn.commit()
    conn.close()
    logger.info("Load complete, connection closed.")