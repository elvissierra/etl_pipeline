import pandas as pd
import re
from auto_report_pipeline.utils import clean_list_string
import numpy as np
import csv

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

    flags = [
        "aggregate",
        "root_only",
        "separate_nodes",
        "average",
        "duplicate",
        "clean",
    ]
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

    sections = []
    sections.append([["Total rows", "", total_config]])

    for col_name in cfg["column"].unique():
        if col_name not in report_df.columns:
            continue

        is_clean = cfg.loc[cfg["column"] == col_name, "clean"].any()
        if is_clean:
            clean_section = [[col_name.replace("_", " ").upper(), "", "Cleaned"]]
            cleaned = report_df[col_name].apply(clean_list_string)
            for val in cleaned:
                clean_section.append(["", "", val])
            sections.append(clean_section)
            continue

        if cfg.loc[cfg["column"] == col_name, "duplicate"].any():
            raw = report_df[col_name].fillna("").astype(str)
            counts = raw.value_counts()
            duplicate = counts[counts > 1]
            section = [[col_name.replace("_", " ").upper(), "Duplicates", "Instances"]]
            for value, cnt in duplicate.items():
                section.append(["", value, cnt])
            sections.append(section)
            continue

        if cfg.loc[cfg["column"] == col_name, "average"].any():
            raw = report_df[col_name].fillna("").astype(str)
            if not raw.str.match(r"^\d+(\.\d+)?%?$").all():
                sections.append(
                    [
                        [col_name.replace("_", " ").upper(), "", "Average"],
                        ["Non-digit field", "", ""],
                    ]
                )
            else:
                nums = pd.to_numeric(raw.str.rstrip("%"), errors="coerce")
                avg = nums.mean()
                unit = "%" if raw.str.endswith("%").any() else ""
                sections.append(
                    [
                        [col_name.replace("_", " ").upper(), "", "Average"],
                        ["", "", f"{avg:.2f}{unit}"],
                    ]
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
                        )
                        .explode()
                        .dropna()
                        .str.strip()
                        .str.lower()
                    )
                    for val in items:
                        label = val or "None"
                        label_counts[label] = label_counts.get(label, 0) + 1
                elif r["aggregate"]:
                    if r["root_only"]:
                        series = series.str.split(
                            re.escape(r["delimiter"]), expand=True
                        )[0]
                    for val in sorted(series.str.strip().str.lower().unique()):
                        if not val.strip():
                            continue  # Skip empty/blank values after cleaning
                        label = val
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

        section = [[col_name.replace("_", " ").upper(), "%", "Count"]]
        for label, cnt in label_counts.items():
            pct = round(cnt / total_rows * 100, 2)
            section.append([label, f"{pct:.2f}%", cnt])
        sections.append(section)

    return sections

# =========================
# Lightweight Insights (correlations + crosstabs)
# =========================

def is_categorical_column(series: pd.Series, max_unique_values: int = 20) -> bool:
    """Treat as categorical if dtype is object or low cardinality numeric."""
    try:
        unique_count = series.nunique(dropna=True)
    except Exception:
        unique_count = max_unique_values + 1
    return series.dtype == "object" or unique_count <= max_unique_values


def cramers_v_stat(col_a: pd.Series, col_b: pd.Series) -> float:
    """Cramér's V for two categorical columns (no SciPy dependency)."""
    contingency_table = pd.crosstab(col_a, col_b)
    if contingency_table.shape[0] < 2 or contingency_table.shape[1] < 2:
        return np.nan

    observed = contingency_table.values.astype(float)
    total = observed.sum()
    if total == 0:
        return np.nan

    row_totals = observed.sum(axis=1, keepdims=True)
    col_totals = observed.sum(axis=0, keepdims=True)
    expected = row_totals @ col_totals / total

    with np.errstate(divide="ignore", invalid="ignore"):
        chi_square = np.nansum((observed - expected) ** 2 / expected)

    rows, cols = observed.shape
    phi2 = chi_square / total

    # Bias correction
    phi2_corrected = max(0.0, phi2 - ((cols - 1) * (rows - 1)) / (total - 1))
    rows_corrected = rows - ((rows - 1) ** 2) / (total - 1)
    cols_corrected = cols - ((cols - 1) ** 2) / (total - 1)
    denom = min((cols_corrected - 1), (rows_corrected - 1))

    return (np.sqrt(phi2_corrected / denom) if denom > 0 else np.nan)


def compute_correlations_and_crosstabs(
    dataframe: pd.DataFrame,
    source_columns: list,
    target_columns: list,
    correlation_threshold: float = 0.2,
    crosstab_output_path: str = "auto_report_pipeline/csv_files/crosstabs_output.csv",
    correlations_output_path: str = "auto_report_pipeline/csv_files/correlation_results.csv",
    verbose: bool = True,
) -> pd.DataFrame:
    """Compare selected columns and persist crosstabs and strongest correlations.

    - Numeric↔Numeric: Pearson
    - Categorical↔Categorical: Cramér's V (+ write crosstabs)
    - Mixed: one-hot categorical, Pearson vs numeric, keep max |r|
    """
    from pandas.api.types import is_numeric_dtype

    correlation_rows = []

    available_sources = [c for c in source_columns if c in dataframe.columns]
    available_targets = [c for c in target_columns if c in dataframe.columns]

    if verbose:
        missing_sources = sorted(set(source_columns) - set(available_sources))
        missing_targets = sorted(set(target_columns) - set(available_targets))
        if missing_sources:
            print(f"[insights] Skipping missing source columns: {missing_sources}")
        if missing_targets:
            print(f"[insights] Skipping missing target columns: {missing_targets}")

    with open(crosstab_output_path, mode="w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)

        for src_col in available_sources:
            for tgt_col in available_targets:
                src_series = dataframe[src_col]
                tgt_series = dataframe[tgt_col]

                mask = src_series.notna() & tgt_series.notna()
                if mask.sum() == 0:
                    continue

                src_vals = src_series[mask]
                tgt_vals = tgt_series[mask]

                try:
                    # Numeric ↔ Numeric
                    if is_numeric_dtype(src_vals) and is_numeric_dtype(tgt_vals):
                        pearson = src_vals.corr(tgt_vals)
                        if pearson is not None and np.isfinite(pearson) and abs(pearson) >= correlation_threshold:
                            correlation_rows.append({
                                "Source Column": src_col,
                                "Target Column": tgt_col,
                                "Correlation": round(float(pearson), 4),
                                "Method": "Pearson",
                                "Type": "Positive" if pearson > 0 else "Negative",
                            })

                    # Categorical ↔ Categorical
                    elif is_categorical_column(src_vals) and is_categorical_column(tgt_vals):
                        ctab = pd.crosstab(src_vals, tgt_vals)
                        writer.writerow([f"=== Crosstab: {src_col} vs {tgt_col} ==="])
                        writer.writerow([ctab.index.name or src_col] + list(ctab.columns))
                        for idx, row in ctab.iterrows():
                            writer.writerow([idx] + list(row.values))
                        writer.writerow([])

                        v = cramers_v_stat(src_vals, tgt_vals)
                        if v is not None and np.isfinite(v) and v >= correlation_threshold:
                            correlation_rows.append({
                                "Source Column": src_col,
                                "Target Column": tgt_col,
                                "Correlation": round(float(v), 4),
                                "Method": "Cramér's V",
                                "Type": "N/A",
                            })

                    # Mixed (Categorical ↔ Numeric)
                    elif (is_categorical_column(src_vals) and is_numeric_dtype(tgt_vals)) or \
                         (is_numeric_dtype(src_vals) and is_categorical_column(tgt_vals)):
                        cat_vals, num_vals = (src_vals, tgt_vals) if is_categorical_column(src_vals) else (tgt_vals, src_vals)
                        dummies = pd.get_dummies(cat_vals)
                        max_abs_corr = dummies.corrwith(num_vals).abs().max() if not dummies.empty else 0.0
                        if verbose:
                            print(f"[insights] {src_col} vs {tgt_col}: mixed max |r|={max_abs_corr:.4f}")
                        if max_abs_corr is not None and np.isfinite(max_abs_corr) and max_abs_corr >= correlation_threshold:
                            correlation_rows.append({
                                "Source Column": src_col,
                                "Target Column": tgt_col,
                                "Correlation": round(float(max_abs_corr), 4),
                                "Method": "Dummies→Pearson",
                                "Type": "Mixed",
                            })
                except Exception as e:
                    if verbose:
                        print(f"[insights] Skipped {src_col} vs {tgt_col}: {e}")
                    continue

    results_df = pd.DataFrame(correlation_rows)
    results_df = results_df.sort_values(by="Correlation", ascending=False) if not results_df.empty else results_df
    results_df.to_csv(correlations_output_path, index=False)

    print(f"✅ Correlation results → {correlations_output_path}")
    print(f"✅ Crosstabs written → {crosstab_output_path}")

    return results_df


def run_basic_insights(
    dataframe: pd.DataFrame,
    threshold: float = 0.2,
    output_dir: str = "auto_report_pipeline/csv_files",
):
    """Run minimal correlations if expected columns are present; write outputs next to report."""
    # Candidate columns; only those present will be used
    source_candidates = [
        "Country",
        "Modern Category",
        "Popularity",
        "Supports Apple Pay",
    ]
    target_candidates = [
        "Are hours visiting hours for tourists?",
        "Is POI also a tourist attraction?",
        "Are Hours Seasonal?",
        "Religious Category Flag",
    ]

    # Try to engineer a popularity bin if Popularity exists and is numeric
    if "Popularity" in dataframe.columns:
        try:
            dataframe["Popularity"] = pd.to_numeric(dataframe["Popularity"], errors="coerce")
            if dataframe["Popularity"].notna().sum() > 0:
                dataframe["pop_bin"] = pd.qcut(dataframe["Popularity"], q=3, labels=["Low", "Med", "High"])
                source_candidates.append("pop_bin")
        except Exception:
            pass

    available_sources = [c for c in source_candidates if c in dataframe.columns]
    available_targets = [c for c in target_candidates if c in dataframe.columns]

    if not available_sources or not available_targets:
        print("[insights] Skipping: required columns not found in DataFrame.")
        return None

    crosstab_path = f"{output_dir}/crosstabs_output.csv"
    correlation_path = f"{output_dir}/correlation_results.csv"

    return compute_correlations_and_crosstabs(
        dataframe,
        available_sources,
        available_targets,
        correlation_threshold=threshold,
        crosstab_output_path=crosstab_path,
        correlations_output_path=correlation_path,
    )
