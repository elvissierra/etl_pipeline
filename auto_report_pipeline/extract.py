import pandas as pd
import numpy as np

def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    
    if "column" in df.columns:
        df["column"] = df["column"].astype(str).str.strip().str.lower().str.replace(" ", "_")

    df = df.replace(r"^\s*$", np.nan, regex=True)
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    return df