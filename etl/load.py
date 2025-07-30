import sqlite3

def load_data(df, db_path, table_name, table_schema):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Construct CREATE TABLE SQL
    schema_parts = [f"{col} {dtype}" for col, dtype in table_schema.items()]
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join(schema_parts)}
    )"""
    cursor.execute(create_table_sql)

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
