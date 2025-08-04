from auto_report_pipeline.extract import load_csv
from auto_report_pipeline.transform import generate_column_report
from auto_report_pipeline.report_generator import assemble_report, save_report

# Coupled with auto_report_pipeline dir

def run_auto_report(input_path: str, config_path: str, output_path: str):
    df = load_csv(input_path)
    config_df = load_csv(config_path)

    report_blocks = generate_column_report(df, config_df)
    final_report = assemble_report(report_blocks)
    save_report(final_report, output_path)

if __name__ == "__main__":
    run_auto_report(
        input_path="auto_report_pipeline/csv_files/Test_Report.csv",
        config_path="auto_report_pipeline/csv_files/report_config.csv",
        output_path="auto_report_pipeline/csv_files/Analytics_Report.csv"
    )