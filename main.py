#mix up not to be used alonside any present modules
import logging
import argparse

from etl.logger import setup_logger
from etl.extract import extract_data
from etl.transform import transform_data, validate_schema
from etl.load import load_data
from config.config import (
    DB_PATH, DATA_PATH, REQUIRED_COLUMNS, FILTER_COLUMN, FILTER_VALUE,
    TABLE_NAME, TABLE_SCHEMA
)

# Coupled with etl dir

def run_etl(input_path=None, output_path=None):
    setup_logger()
    try:
        df = extract_data(input_path or DATA_PATH)
        validate_schema(df, REQUIRED_COLUMNS)
        df = transform_data(df, required_cols=REQUIRED_COLUMNS, filter_col=FILTER_COLUMN, filter_value=FILTER_VALUE)

        if df.empty:
            logging.warning("No data to load after transformation")
            return

        load_data(df, output_path or DB_PATH, TABLE_NAME, TABLE_SCHEMA)
        logging.info("ETL pipeline ran successfully.")

    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="Path to input CSV")
    parser.add_argument("--output", help="Path to SQLite DB")
    args = parser.parse_args()

    run_etl(args.input, args.output)
