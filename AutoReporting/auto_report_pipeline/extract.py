
import pandas as pd
import numpy as np


def _normalize_headers(cols: pd.Index) -> pd.Index:
    return cols.str.strip().str.lower().str.replace(" ", "_", regex=False)


def load_csv(path: str) -> pd.DataFrame:
    """
    CSV loader used for BOTH data and config files.
    Behavior:
    1) First, try normal header=0 read.
    2) If the resulting columns do NOT include 'column' but the file actually
       contains a header row for the report config further down, re-parse by
       auto-detecting the row where the first cell equals 'COLUMN' (case-insensitive),
       using that row as the header.
    3) Normalize headers and basic whitespace/blank handling.
    """
    df = pd.read_csv(path)
    df.columns = _normalize_headers(df.columns)

    if "column" not in df.columns:
        raw = pd.read_csv(path, header=None, dtype=str, keep_default_na=False)
        raw = raw.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))
        first_col = raw.iloc[:, 0].astype(str).str.strip().str.lower()
        header_row_idx = first_col[first_col == "column"].index.tolist()
        if header_row_idx:
            hdr_idx = header_row_idx[0]
            header = raw.iloc[hdr_idx].astype(str)
            header = header.str.strip().str.lower().str.replace(" ", "_", regex=False)
            data = raw.iloc[hdr_idx + 1 :].copy()
            data = data.iloc[:, : len(header)]
            data.columns = header
            df = data

    if "column" in df.columns:
        df["column"] = (
            df["column"].astype(str).str.strip().str.lower().str.replace(" ", "_", regex=False)
        )

    df = df.replace(r"^\s*$", np.nan, regex=True)
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    return df
