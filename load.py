import sqlite3

from config import DB_PATH

# Data Loading
def load_data(transformed_data, db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # Create table with SQL
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS report_data (
        place_id INTEGER PRIMARY KEY,
        edited_fields TEXT,
        last_editor_resolution TEXT,
        suggested_fields TEXT,
        ticket_type TEXT,
        other_markings_made_along_with_procedural_marking TEXT,
        all_customer_suggested_fields_edited TEXT,
        popularity INTEGER
    )
    """)
    # Insert data
    for _, row in transformed_data.iterrows():
        cursor.execute("""
            INSERT OR IGNORE INTO report_data (
                place_id,
                edited_fields,
                last_editor_resolution,
                suggested_fields,
                ticket_type,
                other_markings_made_along_with_procedural_marking,
                all_customer_suggested_fields_edited,
                popularity
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row["place_id"],
            row["edited_fields"],
            row["last_editor_resolution"],
            row["suggested_fields"],
            row["ticket_type"],
            row["other_markings_made_along_with_procedural_marking"],
            row["all_customer_suggested_fields_edited"],
            row["popularity"]
        ))
    conn.commit()
    conn.close()
