import pandas as pd


def extract_data(file_path=None):
    if not file_path:
        from config.config import DATA_PATH
        file_path = DATA_PATH
    return pd.read_csv(file_path)