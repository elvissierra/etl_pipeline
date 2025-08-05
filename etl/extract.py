import pandas as pd
from etl.logger import setup_logger
logger = setup_logger(__name__)


def extract_data(file_path: str):
    logger.info("Starting data extraction from %s", file_path)
    if not file_path:
        logger.error("No file_path provided to extract_data()")
        raise ValueError("'file_path' must be provided.")
    df = pd.read_csv(file_path)
    logger.info("Extraction complete: loaded %d rows", len(df))
    return df