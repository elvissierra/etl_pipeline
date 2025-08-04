import pandas as pd


def extract_data(file_path: str):
    if not file_path:
        raise ValueError("'file_path' must be provided.")
    return pd.read_csv(file_path)