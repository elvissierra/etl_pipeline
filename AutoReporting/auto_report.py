# To be used with extract, report_generator, transform, utils
from auto_report_pipeline.extract import load_csv
from auto_report_pipeline.transform import generate_column_report, run_basic_insights
from auto_report_pipeline.report_generator import assemble_report, save_report

import argparse
import os


# Coupled with auto_report_pipeline dir
ANALYTICS_ENABLED = True


def run_auto_report(input_path: str, config_path: str, output_path: str):
    df = load_csv(input_path)
    config_df = load_csv(config_path)

    report_blocks = generate_column_report(df, config_df)
    final_report = assemble_report(report_blocks)
    save_report(final_report, output_path)
    if ANALYTICS_ENABLED:
        try:
            import os
            out_dir = os.path.dirname(output_path) or "."
            run_basic_insights(df, config_df=config_df, output_dir=out_dir)
        except Exception as e:
            print(f"[insights] Skipped due to error: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run auto report pipeline")
    parser.add_argument("--input-path", default="auto_report_pipeline/csv_files/Test_Report.csv", help="Path to input data CSV")
    parser.add_argument("--config-path", default="auto_report_pipeline/csv_files/report_config.csv", help="Path to report_config CSV")
    parser.add_argument("--output-path", default="auto_report_pipeline/csv_files/Analytics_Report.csv", help="Path to write Analytics_Report.csv")
    args = parser.parse_args()

    # Ensure output directory exists
    os.makedirs(os.path.dirname(args.output_path) or ".", exist_ok=True)

    run_auto_report(
        input_path=args.input_path,
        config_path=args.config_path,
        output_path=args.output_path,
    )
