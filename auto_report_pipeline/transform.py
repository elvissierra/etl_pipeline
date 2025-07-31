import pandas as pd
from collections import Counter
from utils import split_values, get_root_value

def generate_column_report(df: pd.DataFrame, config_df: pd.DataFrame) -> list:
    sections = []
    sections.append([["Total rows", "", len(config_df)]])

    for _, cfg_row in config_df.iterrows():
        col = cfg_row["column"]
        value_filter = str(cfg_row.get("value", "")).lower()
        do_agg = str(cfg_row.get("aggregate", "")).lower() in ["yes", "true"]
        root_only = str(cfg_row.get("root_only", "")).lower() in ["yes", "true"]
        delimiter = str(cfg_row.get("delimiter", ",")) or ","
        do_sep = str(cfg_row.get("separate_nodes", "")).lower() in ["yes", "true"]
        do_avg = str(cfg_row.get("average", "")).lower() in ["yes", "true"]
        do_dup = str(cfg_row.get("duplicate", "")).lower() in ["yes", "true"]

        section = [[col.upper(), "", ""]]
        raw_series = df[col].dropna().astype(str)

        if root_only:
            raw_series = raw_series.map(lambda val: get_root_value(val, delimiter))

        if do_sep:
            all_values = []
            for val in raw_series:
                all_values.extend(split_values(val, delimiter))
            raw_series = pd.Series(all_values)

        if value_filter and value_filter != "nan":
            raw_series = raw_series[raw_series.str.lower() == value_filter]

        if do_avg:
            numeric = pd.to_numeric(raw_series, errors="coerce").dropna()
            if not numeric.empty:
                section.append(["AVG", "", round(numeric.mean(), 2)])
            sections.append(section)
            continue

        if do_dup:
            counts = raw_series.value_counts()
            dupes = counts[counts > 1]
            section.append(["Duplicate", "", ""])
            for val, count in dupes.items():
                section.append([val, "", count])
            sections.append(section)
            continue

        if do_agg:
            counts = raw_series.value_counts()
            total = len(raw_series)
            for val, count in counts.items():
                pct = f"{100 * count / total:.1f}%"
                section.append([val, pct, count])
            sections.append(section)
            continue

        # Default: just count occurrences
        counts = raw_series.value_counts()
        for val, count in counts.items():
            section.append([val, "", count])
        sections.append(section)

    return sections