import pandas as pd
from etl.logger import setup_logger
logger = setup_logger(__name__)


def validate_schema(df: pd.DataFrame, required_cols):
    logger.info("Validating schema: checking for colums %s", required_cols)
    missing = set(required_cols) - set(df.columns)
    if missing:
        logger.error("Error schema validation failed; missing columns: %s", missing)
        raise ValueError(f"Missing required columns: {missing}")
    logger.info("Schema validation passed")