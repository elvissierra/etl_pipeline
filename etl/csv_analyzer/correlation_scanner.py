import pandas as pd
import numpy as np
from scipy.stats import chi2_contingency
from scipy.stats import spearmanr
from itertools import product
from vendor_list import vendors_list
from collections import Counter, defaultdict
from pandas.api.types import is_numeric_dtype
import re
import csv


def analyze_vendor_column(df, vendor_col_name, vendors_list):
    vendor_counts = Counter()
    vendor_duplicates = Counter()
    vendor_duplicate_rows = defaultdict(list)

    # Preprocess vendor names into regex patterns (word-boundary locked, case-insensitive)
    vendor_patterns = {
        vendor: re.compile(rf"\b{re.escape(vendor)}\b", flags=re.IGNORECASE)
        for vendor in vendors_list
    }

    for idx, cell in enumerate(df[vendor_col_name].dropna(), start=0):
        cell_str = str(cell)
        for vendor, pattern in vendor_patterns.items():
            matches = pattern.findall(cell_str)
            count = len(matches)

            if count > 0:
                vendor_counts[vendor] += count
                if count > 1:
                    vendor_duplicates[vendor] += 1
                    vendor_duplicate_rows[vendor].append(
                        idx + 2
                    )  # row num, assuming header

    return vendor_counts, vendor_duplicates, vendor_duplicate_rows


def export_vendor_stats_to_csv(
    vendor_counts, vendor_duplicates, vendor_dup_rows, output_path
):
    data = []
    all_vendors = set(vendor_counts) | set(vendor_duplicates)

    for vendor in sorted(all_vendors):
        total = vendor_counts.get(vendor, 0)
        dup_count = vendor_duplicates.get(vendor, 0)
        dup_rows = ", ".join(map(str, vendor_dup_rows.get(vendor, [])))
        data.append(
            {
                "Vendor": vendor,
                "Total Appearances": total,
                "Rows with Duplicates": dup_count,
                "Duplicate Row Numbers": dup_rows,
            }
        )

    df_export = pd.DataFrame(data)
    df_export.sort_values(by="Total Appearances", ascending=False, inplace=True)
    df_export.to_csv(output_path, index=False)
    print(f"✅ Vendor stats exported to: {output_path}")


def extract_vendors_with_duplicates(cell, vendors_list):
    if pd.isna(cell):
        return []

    found = []
    cell_lower = cell.lower()
    for vendor in vendors_list:
        count = cell_lower.count(vendor.lower())
        found.extend([vendor] * count)  # preserve all duplicates
    return found


def cramers_v(x, y):
    confusion_matrix = pd.crosstab(x, y)
    if confusion_matrix.shape[0] < 2 or confusion_matrix.shape[1] < 2:
        return np.nan
    chi2, _, _, _ = chi2_contingency(confusion_matrix, correction=False)
    n = confusion_matrix.sum().sum()
    if n == 0:
        return np.nan
    phi2 = chi2 / n
    r, k = confusion_matrix.shape
    with np.errstate(divide="ignore", invalid="ignore"):
        phi2corr = max(0, phi2 - ((k - 1) * (r - 1)) / (n - 1))
        rcorr = r - ((r - 1) ** 2) / (n - 1)
        kcorr = k - ((k - 1) ** 2) / (n - 1)
        denom = min((kcorr - 1), (rcorr - 1))
        return np.sqrt(phi2corr / denom) if denom > 0 else np.nan


def is_categorical(series, max_unique=20):
    return series.dtype == "object" or series.nunique() <= max_unique


def compare_source_editor_columns(
    df, source_cols, editor_cols, threshold=0.2, verbose=False
):
    results = []
    output_path = "ETL_Pipeline/etl/csv_analyzer/output_data/crosstabs_output.csv"

    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        for src_col, edt_col in product(source_cols, editor_cols):
            s1, s2 = df[src_col], df[edt_col]
            print(f"Comparing {src_col} vs {edt_col}... ", end="")
            valid = s1.notna() & s2.notna()
            print(f"{valid.sum()} valid rows")

            # if valid.sum() < 5:
            #    continue

            s1, s2 = s1[valid], s2[valid]

            # Add crosstab to CSV if both categorical
            if is_categorical(s1) and is_categorical(s2):
                ctab = pd.crosstab(s1, s2)
                writer.writerow([f"=== Crosstab: {src_col} vs {edt_col} ==="])
                writer.writerow([ctab.index.name or src_col] + list(ctab.columns))
                for idx, row in ctab.iterrows():
                    writer.writerow([idx] + list(row.values))
                writer.writerow([])  # Blank line

            # Now calculate correlation if needed
            try:
                if np.issubdtype(s1.dtype, np.number) and np.issubdtype(
                    s2.dtype, np.number
                ):
                    rho, pval = spearmanr(s1, s2)
                    print(f"   Spearman rho={rho:.4f}, p={pval:.3g}")
                    r = s1.corr(s2)
                    if abs(r) >= threshold:
                        results.append(
                            {
                                "Source Column": src_col,
                                "Editor Column": edt_col,
                                "Correlation": round(r, 4),
                                "Method": "Pearson",
                                "Type": "Positive" if r > 0 else "Negative",
                            }
                        )

                elif is_categorical(s1) and is_categorical(s2):
                    v = cramers_v(s1, s2)
                    if v >= threshold:
                        results.append(
                            {
                                "Source Column": src_col,
                                "Editor Column": edt_col,
                                "Correlation": round(v, 4),
                                "Method": "Cramér's V",
                                "Type": "N/A",
                            }
                        )

                # elif (is_categorical(s1) and np.issubdtype(s2.dtype, np.number)) or \
                #      (np.issubdtype(s1.dtype, np.number) and is_categorical(s2)):
                #     print("===================FLAGGING TO LET ME KNOW IF THIS IS GETTING TRIGGERED =======================")
                #     if is_categorical(s1):
                #         dummies = pd.get_dummies(s1)
                #         max_corr = dummies.corrwith(s2).abs().max()
                #     else:
                #         dummies = pd.get_dummies(s2)
                #         max_corr = dummies.corrwith(s1).abs().max()

                #     if max_corr >= threshold:
                #         results.append({
                #             'Source Column': src_col,
                #             'Editor Column': edt_col,
                #             'Correlation': round(max_corr, 4),
                #             'Method': 'Dummies→Pearson',
                #             'Type': 'Mixed'
                #         })

                elif (is_categorical(s1) and is_numeric_dtype(s2)) or (
                    is_numeric_dtype(s1) and is_categorical(s2)
                ):

                    # Identify which is which
                    cat, num = (s1, s2) if is_categorical(s1) else (s2, s1)
                    prefix = "Computing mixed corr for"
                    print(
                        f"{prefix} {src_col if cat is s1 else edt_col} (categorical) vs "
                        f"{edt_col if num is s2 else src_col} (numeric)"
                    )

                    dummies = pd.get_dummies(cat)
                    corr_series = dummies.corrwith(num)
                    abs_corrs = corr_series.abs()

                    print(" → Dummy→Pearson correlations:")
                    print(abs_corrs.to_frame("r_value"))

                    max_corr = abs_corrs.max() if not abs_corrs.empty else 0
                    print(f" → max_corr = {max_corr:.4f}")

                    if max_corr >= threshold:
                        results.append(
                            {
                                "Source Column": src_col,
                                "Editor Column": edt_col,
                                "Correlation": round(max_corr, 4),
                                "Method": "Dummies→Pearson",
                                "Type": "Mixed",
                            }
                        )

            except Exception as e:
                if verbose:
                    print(f"Skipped {src_col} vs {edt_col}: {e}")
                continue

    print(f"✅ Crosstabs written cleanly to {output_path}")

    # Return correlation results if needed
    results_df = pd.DataFrame(results)
    if results_df.empty:
        print("⚠️ No strong correlations found.")
        return results_df
    return results_df.sort_values(by="Correlation", ascending=False)
