# Designing ETL Pipeline

#- Data source- csv
#- Transformation- include cleaning(removing null values) and filtering (based on criteria)
#- Data Destination- database, csv, or any other storage

import pandas as pd
import sqlite3
import time

# Data Extraction
def extract_data(file_path="data/Test_Data.csv"):
    data = pd.read_csv(file_path)
    return data

# Data Transformation
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    # Remove rows with any nulls in required columns
    cleaned = df.dropna(subset=[
        "place_id", "edited_fields", "last_editor_resolution", "suggested_fields", "ticket_type", "other_markings_made_along_with_procedural_marking", "all_customer_suggested_fields_edited", "popularity"
    ])
    # Filter out rows where all_customer_suggested_fields_edited == "Yes"
    filtered = cleaned[cleaned["all_customer_suggested_fields_edited"] != "Yes"]
    return filtered

# Data Loading
def load_data(transformed_data, db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # Create table with correct SQL syntax
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

# Pipeline Orchestration
def run_etl_pipeline():
    try:
        data = extract_data()
        transformed_data = transform_data(data)
        load_data(transformed_data, "data/report_data.db")
        print("ETL pipeline run successfully.")
    except Exception as e:
        print(f"Error occurred: {e}")

# Scheduling (run once per day)
if __name__ == "__main__":
    while True:
        run_etl_pipeline()
        time.sleep(86400)


