import pandas as pd

def validate_schema(df: pd.DataFrame, required_cols):
    missing = set(required_cols) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")