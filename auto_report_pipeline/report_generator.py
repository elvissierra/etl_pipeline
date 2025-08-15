import pandas as pd


def assemble_report(sections: list) -> pd.DataFrame:
    """
    Combines all report blocks into a long-form DataFrame.
    """
    final_rows = []
    for block in sections:
        final_rows.extend(block + [["", "", ""]])
    return pd.DataFrame(final_rows)


def save_report(df: pd.DataFrame, output_path: str):
    df.to_csv(output_path, index=False, header=False)
    print(f"✅ Report saved to {output_path}")
