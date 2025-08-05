import pandas as pd
from etl.logger import setup_logger
logger = setup_logger(__name__)


# --- Schema-based row dropping ---
def drop_missing(df: pd.DataFrame, required_cols: list) -> pd.DataFrame:
    """
    Drop rows where any of the required columns are missing (NaN).
    """
    logger.info("Dropping rows missing required columns: %s", required_cols)
    before = len(df)
    df = df.dropna(subset=required_cols)
    after = len(df)
    logger.info("Dropped %d rows; %d remaining", before - after, after)
    return df


def transform_data(
    df: pd.DataFrame,
    filter_col=None,
    filter_value=None
) -> pd.DataFrame:
    logger.info("Starting transformation step")
    if filter_col and filter_value is not None:
        logger.info("Filtering out rows where %s == %s", filter_col, filter_value)
        before, after = len(df), len(df[df[filter_col] != filter_value])
        df = df[df[filter_col] != filter_value]
        logger.info("Filter applied: %d->%d rows", before, after)
    logger.info("Transform complete")
    return df
