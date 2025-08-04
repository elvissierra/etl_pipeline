import pandas as pd

def transform_data(
    df: pd.DataFrame,
    required_cols=None,
    filter_col=None,
    filter_value=None
):
    if required_cols:
        df = df.dropna(subset=required_cols)
    if filter_col and filter_value is not None:
        df = df[df[filter_col] != filter_value]
    return df
