import pandas as pd
import re
from AR_ETL import clean_list_string

"""
COLUMN
Is the column in the report to be manipulated.
-------------------------------------
VALUE
Is an optional input that will target a single value present within the reporting file.
-------------------------------------
AGGREGATE
To always be set as “yes” unless when blank/NaN value is desired. # Is there a better use case?
-------------------------------------
ROOT ONLY
When the desired output is at index[0]. # this is for when the desired output is the 1st word or index of a string
-------------------------------------
DELIMITER
Any character to separate by.
-------------------------------------
SEPARATE NODES
To be used for when there are nested values and alongside DELIMITER.
-------------------------------------
DUPLICATE
To identify duplicates.
-------------------------------------
AVERAGE
To output the average within a column this is float int any other digit base.
-------------------------------------
CLEAN
To clean any marking from a string, this is to only output character
"""


def generate_column_report(report_df: pd.DataFrame, config_df: pd.DataFrame) -> list:
    total_config = len(config_df)
    total_rows = len(report_df)
    cfg = config_df.copy()
    cfg.columns = cfg.columns.str.strip().str.lower().str.replace(" ", "_")
    cfg["column"] = cfg["column"].astype(str).str.strip()

    flags = ["aggregate", "root_only", "separate_nodes", "average", "duplicate", "clean"]
    for flag in flags:
        if flag in cfg.columns:
            cfg[flag] = (
                cfg[flag]
                    .fillna("False")
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    .isin(["yes", "true"])
            )
        else:
            cfg[flag] = "False"

    if "value" in cfg.columns:
        cfg["value"] = cfg["value"].fillna("").astype(str).str.lower()
    else:
        cfg["value"] = ""

    if "delimiter" in cfg.columns:
        cfg["delimiter"] = cfg["delimiter"].fillna("|").astype(str)
    else:
        cfg["delimiter"] = ""

    for clean_col in cfg.loc[cfg["clean"], "column"]:
        report_df[clean_col] = (
            report_df[clean_col]
            .fillna("")
            .astype(str)
            .apply(clean_list_string)
        )

    sections = []
    sections.append([["Total rows", "", total_config]])

    for col_name in cfg["column"].unique():
        if col_name not in report_df.columns:
            continue

        if cfg.loc[cfg["column"] == col_name, "duplicate"].any():
            raw = report_df[col_name].fillna("").astype(str)
            counts = raw.value_counts()
            duplicate =  counts[counts > 1]
            section = [[col_name.upper(), "", "Duplicate"]]
            for value, cnt in duplicate.items():
                section.append([value, "", cnt])
            sections.append(section)
            continue

        if cfg.loc[cfg["column"] == col_name, "average"].any():
            raw = report_df[col_name].fillna("").astype(str)
            if not raw.str.match(r"^\d+(\.\d+)?%?$").all():
                sections.append(
                    [[col_name.upper(), "", "Average"], ["Non-digit field", "", ""]]
                )
            else:
                nums = pd.to_numeric(raw.str.rstrip("%"), errors="coerce")
                avg = nums.mean()
                unit = "%" if raw.str.endswith("%").any() else ""
                sections.append(
                    [[col_name.upper(), "", "Average"], ["", "", f"{avg:.2f}{unit}"]]
                )
            continue

        entries = cfg[cfg["column"] == col_name]
        label_counts = {}
        search_value = entries[entries["value"] != ""]

        if not search_value.empty:
            for _, r in search_value.iterrows():
                series = report_df[col_name].fillna("").astype(str)
                if r["separate_nodes"]:
                    items = (
                        series.str.split(
                            rf"\s*{re.escape(r["delimiter"])}\s*", regex=True
                        )
                        .explode()
                        .dropna()
                        .str.strip()
                        .str.lower()
                        .apply(clean_list_string)
                    )
                    cnt = int((items == r["value"]).sum())
                else:
                    if r["root_only"]:
                        series = series.str.split(
                            re.escape(r["delimiter"]), expand=True
                        )[0]
                    pattern = rf"(?:^|\|)\s*{re.escape(r["value"])}\s*(?:\||$)"
                    cnt = int(series.str.lower().str.contains(pattern).sum())
                label = r["value"] or "None"
                label_counts[label] = cnt
        else:
            for _, r in entries.iterrows():
                series = report_df[col_name].fillna("").astype(str)
                if r["separate_nodes"]:
                    items = (
                        series.str.split(
                            rf"\s*{re.escape(r["delimiter"])}\s*", regex=True
                        ).explode().dropna().str.strip().str.lower())
                    for val in items:
                        label = val or "None"
                        label_counts[label] = label_counts.get(label, 0) + 1
                elif r["aggregate"]:
                    if r["root_only"]:
                        series = series.str.split(re.escape(r["delimiter"]), expand=True)[0]
                    for val in sorted(series.str.strip().str.lower().unique()):
                        label = val or "None"
                        cnt = int((series.str.strip().str.lower() == val).sum())
                        label_counts[label] = cnt
                else:
                    if r["root_only"]:
                        series = series.str.split(
                            re.escape(r["delimiter"]), expand=True
                        )[0]
                    pattern = rf"(?:^|\|)\s*{re.escape(r["value"])}\s*(?:\||$)"
                    cnt = int(series.str.lower().str.contains(pattern).sum())
                    label = r["value"] or "None"
                    label_counts[label] = label_counts.get(label, 0) + cnt

        section = [[col_name.upper(), "%", "Count"]]
        for label, cnt in label_counts.items():
            pct = round(cnt / total_rows * 100, 2)
            section.append([label, f"{pct:.2f}%", cnt])
        sections.append(section)

    return sections