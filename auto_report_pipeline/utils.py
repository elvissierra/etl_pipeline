import pandas as pd

def split_values(value: str, delimiter: str) -> list:
    if pd.isna(value):
        return []
    return [v.strip() for v in str(value).split(delimiter) if v.strip()]

def get_root_value(value: str, delimiter: str) -> str:
    return split_values(value, delimiter)[0] if split_values(value, delimiter) else ""

def clean_string(value: str) -> str:
    return str(value).strip() if isinstance(value, str) else value