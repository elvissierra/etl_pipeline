
#- Data source- csv
#- Transformation- include cleaning(removing null values) and filtering (based on criteria)
#- Data Destination- database, csv, or any other storage

import logging
from logger import setup_logger
from extract import extract_data
from transform import transform_data
from load import load_data
from config import DB_PATH


# Pipeline Orchestration
def run_etl():
    setup_logger()
    try:
        df = extract_data()
        df = transform_data(df)
        if df.empty:
            logging.warning("No data to load after transformation")
            return
        load_data(df, DB_PATH)
        logging.info("ETL pipeline ran successfully.")
    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")

if __name__ == "__main__":
        run_etl()
