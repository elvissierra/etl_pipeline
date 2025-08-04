# Not current
import os
import numpy as np
import pandas as pd

# ETL imports
from auto_report_pipeline.extract import load_csv
from auto_report_pipeline.transform import generate_column_report
from auto_report_pipeline.report_generator import assemble_report, save_report

# Analyzer imports
from csv_analyzer.correlation_scanner import (
    analyze_vendor_column,
    export_vendor_stats_to_csv,
    compare_source_editor_columns,
)
from csv_analyzer.pop import run_popularity_logistic
from csv_analyzer.vendor_list import vendors_list

def pipeline_main(
    raw_csv_path,
    report_config_path,
    analytics_report_path,
    vendor_stats_path,
    correlation_output_path,
):
    # ─── 1) ETL: generate Analytics_Report.csv ─────────────────────────────
    df = load_csv(raw_csv_path)
    cfg = load_csv(report_config_path)
    report_blocks = generate_column_report(df, cfg)
    analytics_df = assemble_report(report_blocks)
    save_report(analytics_df, analytics_report_path)

    # ─── 2) Vendor Analysis ────────────────────────────────────────────────
    vendor_counts, vendor_dups, vendor_dup_rows = analyze_vendor_column(
        df, "vendors", vendors_list
    )
    export_vendor_stats_to_csv(
        vendor_counts, vendor_dups, vendor_dup_rows, vendor_stats_path
    )

    # ─── 3) Correlation Scanning ───────────────────────────────────────────
    # You’ll need to define your source/editor columns here, e.g.:
    source_cols = ["country", "modern_category", "popularity"]
    editor_cols = ["is_poi_also_a_tourist_attraction", "are_hours_seasonal"]
    corr_df = compare_source_editor_columns(
        df, source_cols, editor_cols, threshold=0.2, verbose=True
    )
    corr_df.to_csv(correlation_output_path, index=False)
    print(f"✅ Correlations saved to {correlation_output_path}")

    # ─── 4) Popularity Logistic Regression ─────────────────────────────────
    model, odds, prob_df = run_popularity_logistic(df)
    print(model.summary())
    print("\nOdds Ratios:\n", odds)
    print("\nPredicted probabilities:\n", prob_df)

if __name__ == "__main__":
    pipeline_main(
        raw_csv_path="csv_analyzer/input_csv/Test_Report.csv",
        report_config_path="auto_report_pipeline/csv_files/report_config.csv",
        analytics_report_path="auto_report_pipeline/csv_files/Analytics_Report.csv",
        vendor_stats_path="csv_analyzer/output_data/vendor_stats.csv",
        correlation_output_path="csv_analyzer/output_data/correlation_results.csv",
    )