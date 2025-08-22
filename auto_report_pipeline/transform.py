import pandas as pd
import re
from auto_report_pipeline.utils import clean_list_string
import numpy as np
import csv


# Helper for "root_only" delimiter splitting
def _apply_root_only(series: pd.Series, delimiter: str) -> pd.Series:
    """Return the root value (substring before the first delimiter).
    Handles '.' safely and trims whitespace around the delimiter.
    """
    if delimiter is None or str(delimiter) == "":
        return series
    delim = str(delimiter)
    # Build a regex that splits on first occurrence of the delimiter
    if delim == ".":
        pattern = r"\s*\.\s*"
    else:
        pattern = rf"\s*{re.escape(delim)}\s*"
    try:
        return series.astype(str).str.split(pattern, n=1, regex=True).str[0].str.strip()
    except Exception:
        return series

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

    cfg["column"] = (
        cfg["column"].astype(str)
        .str.replace(r"^[\"']+|[\"']+$", "", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    
    def _norm_header(s: str) -> str:
        s = str(s).strip()
        s = re.sub(r"^[\"']+|[\"']+$", "", s)
        s = re.sub(r"\s+", " ", s)
        return s.lower().replace(" ", "_")

    header_lookup = { _norm_header(c): c for c in report_df.columns }

    flags = [
        "aggregate",
        "root_only",
        "separate_nodes",
        "duplicate",
        "average",
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
        resolved_col = header_lookup.get(_norm_header(col_name))
        if not resolved_col:
            continue

        is_clean = cfg.loc[cfg["column"] == col_name, "clean"].any()
        if is_clean:
            clean_section = [[col_name.replace("_", " ").upper(), "", "Cleaned"]]
            cleaned = report_df[resolved_col].apply(clean_list_string)
            for val in cleaned:
                clean_section.append(["", "", val])
            sections.append(clean_section)
            continue

        if cfg.loc[cfg["column"] == col_name, "duplicate"].any():
            raw = report_df[resolved_col].fillna("").astype(str)
            counts = raw.value_counts()
            duplicate = counts[counts > 1]
            section = [[col_name.replace("_", " ").upper(), "Duplicates", "Instances"]]
            for value, cnt in duplicate.items():
                section.append(["", value, cnt])
            sections.append(section)
            continue

        if cfg.loc[cfg["column"] == col_name, "average"].any():
            raw = report_df[resolved_col].fillna("").astype(str)
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
                series = report_df[resolved_col].fillna("").astype(str)
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
                        series = _apply_root_only(series, r["delimiter"])
                    pattern = rf"(?:^|\|)\s*{re.escape(r["value"])}\s*(?:\||$)"
                    cnt = int(series.str.lower().str.contains(pattern).sum())
                label = r["value"] or "None"
                label_counts[label] = cnt
        else:
            for _, r in entries.iterrows():
                series = report_df[resolved_col].fillna("").astype(str)
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
                        series = _apply_root_only(series, r["delimiter"])
                    for val in sorted(series.str.strip().str.lower().unique()):
                        if not val.strip():
                            continue
                        label = val
                        cnt = int((series.str.strip().str.lower() == val).sum())
                        label_counts[label] = cnt
                else:
                    if r["root_only"]:
                        series = _apply_root_only(series, r["delimiter"])
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


# Lightweight Insights (correlations + crosstabs)


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
    include_method: bool = False,
    include_type: bool = False,
) -> pd.DataFrame:
    """Compare selected columns and persist crosstabs and strongest correlations.

    - Numeric↔Numeric: Pearson
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
                            row = {
                                "Source Column": src_col,
                                "Target Column": tgt_col,
                                "Correlation": round(float(pearson), 4),
                            }
                            if include_method:
                                row["Method"] = "Pearson"
                            if include_type:
                                row["Type"] = "Positive" if pearson > 0 else "Negative"
                            correlation_rows.append(row)

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
                            row = {
                                "Source Column": src_col,
                                "Target Column": tgt_col,
                                "Correlation": round(float(v), 4),
                            }
                            if include_method:
                                row["Method"] = "Cramér's V"
                            if include_type:
                                row["Type"] = "N/A"  # Cramér's V is unsigned
                            correlation_rows.append(row)

                    # Mixed (Categorical ↔ Numeric)
                    elif (is_categorical_column(src_vals) and is_numeric_dtype(tgt_vals)) or \
                         (is_numeric_dtype(src_vals) and is_categorical_column(tgt_vals)):
                        cat_vals, num_vals = (src_vals, tgt_vals) if is_categorical_column(src_vals) else (tgt_vals, src_vals)
                        dummies = pd.get_dummies(cat_vals)
                        max_abs_corr = dummies.corrwith(num_vals).abs().max() if not dummies.empty else 0.0
                        if verbose:
                            print(f"[insights] {src_col} vs {tgt_col}: mixed max |r|={max_abs_corr:.4f}")
                        if max_abs_corr is not None and np.isfinite(max_abs_corr) and max_abs_corr >= correlation_threshold:
                            row = {
                                "Source Column": src_col,
                                "Target Column": tgt_col,
                                "Correlation": round(float(max_abs_corr), 4),
                            }
                            if include_method:
                                row["Method"] = "Dummies→Pearson"
                            if include_type:
                                row["Type"] = "Mixed"
                            correlation_rows.append(row)
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


from typing import Optional, Tuple, List, Dict

# Insights directives now accept flexible header names (underscores/spaces not required), e.g.:
#  INSIGHTS ENABLED, INSIGHTS_ENABLED, __INSIGHTS_ENABLED__ (case-insensitive)
def _parse_insights_from_config(config_df: pd.DataFrame) -> Dict[str, object]:
    """
    Extract insights directives from report_config.

    Expected rows (case-insensitive, normalized by extract.load_csv):

    Returns a dict with defaults when rows are missing.
    """
    def _norm_key_name(s: str) -> str:
        """Normalize directive key names.
        Accepts variants like:
        - __INSIGHTS_THRESHOLD__
        - INSIGHTS_THRESHOLD
        - INSIGHTS THRESHOLD
        - insights-threshold (any non-alnum treated as space)
        and resolves to a compact form like 'insightsthreshold'.
        """
        s = str(s or "").strip().lower()
        # Replace any non-alphanumeric with a single space
        s = re.sub(r"[^a-z0-9]+", " ", s)
        # Collapse spaces and drop them
        s = re.sub(r"\s+", " ", s).strip().replace(" ", "")
        return s
    out = {
        "enabled": True,
        "threshold": 0.2,
        "sources": None,
        "targets": None,
    }
    if config_df is None or config_df.empty or "column" not in config_df.columns:
        return out

    # Build lookup from normalized directive key -> value
    rows = config_df[["column", "value"]].copy()
    rows["column"] = rows["column"].astype(str).str.strip()
    rows["value"] = rows["value"].astype(str).str.strip()
    lut = {}
    for _, r in rows.iterrows():
        key_norm = _norm_key_name(r["column"])
        if key_norm in {"insightsenabled", "insightsthreshold", "insightssources", "insightstargets"}:
            lut[key_norm] = r["value"]

    def _as_bool(s: str) -> Optional[bool]:
        s = (s or "").strip().lower()
        if s in {"true", "yes"}: return True
        if s in {"false", "no"}: return False
        return None

    def _as_float(s: str) -> Optional[float]:
        try:
            return float(s)
        except Exception:
            return None

    def _as_list(s: str) -> List[str]:
        if not s:
            return []
        return [part.strip() for part in s.split("|") if part.strip()]

    # enabled
    val = _as_bool(lut.get("insightsenabled", ""))
    if val is not None:
        out["enabled"] = val

    # threshold
    thr = _as_float(lut.get("insightsthreshold", ""))
    if thr is not None:
        out["threshold"] = thr

    # sources / targets
    srcs = _as_list(lut.get("insightssources", ""))
    tgts = _as_list(lut.get("insightstargets", ""))
    if srcs:
        out["sources"] = srcs
    if tgts:
        out["targets"] = tgts

    return out


def run_basic_insights(
    dataframe: pd.DataFrame,
    config_df: Optional[pd.DataFrame] = None,
    threshold: Optional[float] = None,
    output_dir: str = "auto_report_pipeline/csv_files",
):
    """
    Run minimal correlations if expected columns are present; write outputs next to report
    """
    directives = _parse_insights_from_config(config_df)
    if directives.get("enabled") is False:
        print("[insights] Disabled by report_config (__INSIGHTS_ENABLED__=false).")
        return None

    eff_threshold = threshold if threshold is not None else directives.get("threshold", 0.2)

    def _make_unique(cols):
        seen = {}
        out = []
        for c in cols:
            name = str(c)
            if name in seen:
                seen[name] += 1
                out.append(f"{name}.{seen[name]}")
            else:
                seen[name] = 0
                out.append(name)
        return out

    if dataframe.columns.duplicated().any():
        print("[insights] Detected duplicate column names; de-duplicating for analysis.")
        df_work = dataframe.copy()
        df_work.columns = _make_unique(df_work.columns)
    else:
        df_work = dataframe


    source_candidates = directives.get("sources")
    target_candidates = directives.get("targets")


    def _resolve_existing_columns(df: pd.DataFrame, candidates: list[str]) -> tuple[list[str], list[str]]:
        """Resolve candidate names to actual df columns using the SAME normalization
        as extract.load_csv: strip→lower→collapse spaces→underscores.
        Returns (resolved_original_names, missing_candidates).
        """
        if not candidates:
            return [], []

        def _norm(s: str) -> str:
            s = str(s).strip().lower()
            s = re.sub(r"\s+", "_", s)
            return s

        lookup = {}
        for col in df_work.columns:
            key = _norm(col)
            if key not in lookup:
                lookup[key] = col

        resolved, missing = [], []
        for name in candidates:
            key = _norm(name)
            if key in lookup:
                resolved.append(lookup[key])
            else:
                missing.append(name)
        return resolved, missing

    available_sources, missing_sources = _resolve_existing_columns(df_work, source_candidates)
    available_targets, missing_targets = _resolve_existing_columns(df_work, target_candidates)

    if missing_sources:
        print(f"[insights] Missing source columns (after normalization): {missing_sources}")
    if missing_targets:
        print(f"[insights] Missing target columns (after normalization): {missing_targets}")

    if not available_sources or not available_targets:
        print("[insights] Skipping: no usable sources or targets after resolving names.")
        return None

    crosstab_path = f"{output_dir}/crosstabs_output.csv"
    correlation_path = f"{output_dir}/correlation_results.csv"

    return compute_correlations_and_crosstabs(
        df_work,
        available_sources,
        available_targets,
        correlation_threshold=eff_threshold,
        crosstab_output_path=crosstab_path,
        correlations_output_path=correlation_path,
    )
