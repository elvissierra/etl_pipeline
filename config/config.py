import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_USER = os.getenv("DB_USER", "")
DB_PORT = os.getenv("DB_PORT", "")
DATA_PATH = os.getenv("DATA_PATH", "data/Test_Data.csv")
DB_PATH = os.getenv("DB_PATH", "data/report_data.db")
LOG_FILE = os.getenv("LOG_FILE", "etl_errors.log")

# ETL-specific configurations
REQUIRED_COLUMNS = [
    "place_id", "edited_fields", "last_editor_resolution", "suggested_fields",
    "ticket_type", "other_markings_made_along_with_procedural_marking",
    "all_customer_suggested_fields_edited", "popularity"
]

FILTER_COLUMN = "all_customer_suggested_fields_edited"
FILTER_VALUE = "Yes"

TABLE_NAME = "report_data"
TABLE_SCHEMA = {
    "place_id": "INTEGER PRIMARY KEY",
    "edited_fields": "TEXT",
    "last_editor_resolution": "TEXT",
    "suggested_fields": "TEXT",
    "ticket_type": "TEXT",
    "other_markings_made_along_with_procedural_marking": "TEXT",
    "all_customer_suggested_fields_edited": "TEXT",
    "popularity": "INTEGER"
}
