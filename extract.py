import pandas as pd

from config import DATA_PATH

def extract_data(file_path=DATA_PATH):
    return pd.read_csv(file_path)