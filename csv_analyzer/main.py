import os
import numpy as np
import pandas as pd
from correlation_scanner import (
    compare_source_editor_columns,
    analyze_vendor_column,
    export_vendor_stats_to_csv,
)
from vendor_list import vendors_list
from pop import run_popularity_logistic

# === CONFIG ===
input_folder = "csv_analyzer/input_csv"
threshold = 0.2
trace_cols = ["Name", "Place ID"]
verbose = True

# === Find the first CSV file in the folder ===
csv_files = [f for f in os.listdir(input_folder) if f.endswith(".csv")]
if not csv_files:
    raise FileNotFoundError("No CSV file found in the input folder.")
csv_path = os.path.join(input_folder, csv_files[0])

print(f"Loading: {csv_path}")
df = pd.read_csv(csv_path)


# Clean column names

df.columns = df.columns.str.strip().str.replace(r"\s+", " ", regex=True)
df = df.replace(r"^\s*$", np.nan, regex=True)
df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

# fix popularity dtype
df["Popularity"] = pd.to_numeric(df["Popularity"], errors="coerce")

model, odds_ratios, prob_df = run_popularity_logistic(df)

print(model.summary())
print("\nOdds Ratios:\n", odds_ratios)
print("\nPredicted probabilities:\n", prob_df)

bucket_thresholds = [
    (None, "All"),
    (100, "Pop>=100"),
    (250, "Pop>=250"),
    (500, "Pop>=500"),
]

df["pop_log"] = np.log1p(df["Popularity"])

# Detect duplicates
if df.columns.duplicated().any():
    print("⚠️ Duplicate column names detected. Making them unique...")
    df.columns = pd.io.parsers.ParserBase({"names": df.columns})._maybe_dedup_names(
        df.columns
    )

df["pop_bin"] = pd.qcut(df["Popularity"], q=3, labels=["Low", "Med", "High"])
# Then include 'pop_bin' in your source_cols list:


# === Define source and editor columns ===
# source_cols = df.columns[:23]
source_cols = ["Country", "Modern Category", "Popularity", "Supports Apple Pay"]
source_cols.append("pop_bin")
editor_cols = [
    "Are hours visiting hours for tourists?",
    "Is POI also a tourist attraction?",
    "Are Hours Seasonal?",
    "Religious Category Flag",
]
# editor_cols = df.columns[23:]


bucketed_results = []

# for thr, label in bucket_thresholds:
#     if thr is None:
#         df_bucket = df.copy()
#     else:
#         df_bucket = df[df['Popularity'] >= thr]
#     print(f"\n=== Bucket: {label} ({len(df_bucket)} rows) ===")

#     # run your comparison (verbose can be True/False as desired)
#     res = compare_source_editor_columns(
#         df_bucket,
#         source_cols,
#         editor_cols,
#         threshold=threshold,
#         verbose=verbose
#     )
#     if not res.empty:
#         res['Bucket'] = label
#         bucketed_results.append(res)

# # 3. Concatenate & export all in one CSV
# if bucketed_results:
#     all_corrs = pd.concat(bucketed_results, ignore_index=True)
#     out_path = "output_data/correlations_by_bucket.csv"
#     all_corrs.to_csv(out_path, index=False)
#     print(f"✅ All bucketed correlations saved to {out_path}")
# else:
#     print("⚠️ No correlations found in any bucket.")


# 0) Make sure the flag column is clean
flag_col = "Is POI also a tourist attraction?"
df[flag_col] = df[flag_col].astype(str).str.strip()
df[flag_col].replace({"nan": np.nan}, inplace=True)

for thr, label in bucket_thresholds:
    # select your bucket
    if thr is None:
        df_bucket = df.copy()
    else:
        df_bucket = df[df["Popularity"] > thr]

    # 1) drop rows where flag is missing
    df_bucket = df_bucket.dropna(subset=[flag_col])

    # 2) now inspect unique flag values
    print(f"\n=== Bucket: {label} (Popularity > {thr}) ===")
    print("Unique flag values:", df_bucket[flag_col].unique())

    # 3) total rows, and counts per Yes/No/Unverifiable only
    total = len(df_bucket)
    vc = df_bucket[flag_col].value_counts()
    print(f"Total rows in bucket (post‐dropna): {total}")
    print("Flag counts:\n", vc)

    # 4) compute the Yes ratio
    yes = vc.get("Yes", 0)
    print(f"Yes count = {yes}/{total} → {yes/total:.1%}")

vendor_counts, vendor_duplicates, vendor_dup_rows = analyze_vendor_column(
    df, "Vendors", vendors_list
)
export_vendor_stats_to_csv(
    vendor_counts, vendor_duplicates, vendor_dup_rows, "csv_analyzer/output_data/vendor_stats.csv"
)


print("Editor fill counts (non-null per row):")
print(df[editor_cols].notna().sum(axis=1).value_counts())
# === Run correlation scanner ===

# Keep only rows where at least 7 of the 12 editor columns are filled in
min_editor_fields_required = 1
df = df[df[editor_cols].notna().sum(axis=1) >= min_editor_fields_required]

print("Editor columns:", editor_cols)
print("Editor fill counts (non-null per row):")
print(df[editor_cols].notna().sum(axis=1).value_counts())


# vendor_summary = analyze_vendor_frequencies_exact_with_rows(df, vendor_column="Vendors", vendor_list=vendors_list)
# vendor_summary.to_csv("Output_Data/vendor_frequency_summary.csv", index=False)

results = compare_source_editor_columns(
    df, source_cols, editor_cols, threshold=threshold
)


# Compare mean Popularity by tourist flag
print("\nPopularity by Tourist Attraction flag:")
print(df.groupby("Is POI also a tourist attraction?")["Popularity"].describe())

# === Output ===
print("\n=== Correlation Results ===")
print(results)

# Save to CSV
results.to_csv("correlation_results.csv", index=False)
