# To be used with extract, report_generator, transform, utils
from auto_report_pipeline.extract import load_csv
from auto_report_pipeline.transform import generate_column_report, run_basic_insights
from auto_report_pipeline.report_generator import assemble_report, save_report
import glob
import argparse
import os
import csv
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = str(
    (SCRIPT_DIR / "auto_report_pipeline/csv_files/report_config.csv").resolve()
)


# Coupled with auto_report_pipeline dir
ANALYTICS_ENABLED = True


def read_io_from_config(config_path: str) -> tuple[str | None, str | None]:
    """Read INPUT and OUTPUT from the report_config CSV.

    This scans lines *before* the header row that begins with `COLUMN` and
    returns absolute paths resolved relative to the config file's directory.
    """
    input_path = None
    output_path = None
    cfg_dir = Path(config_path).resolve().parent

    with open(config_path, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        for row in reader:
            if not row:
                continue
            key = (row[0] or "").strip().lower()
            if key == "column":
                break
            if key == "input" and len(row) >= 2 and row[1].strip():
                input_path = row[1].strip()
            if key == "output" and len(row) >= 2 and row[1].strip():
                output_path = row[1].strip()

    def _resolve(p: str | None) -> str | None:
        if not p:
            return None
        pth = Path(p)
        # If relative and config.csv is already inside csv_files/, avoid duplicating the segment
        if not pth.is_absolute():
            if (
                cfg_dir.name == "csv_files"
                and len(pth.parts) > 0
                and pth.parts[0] == "csv_files"
            ):
                # Drop the leading 'csv_files' from the provided path
                pth = Path(*pth.parts[1:]) if len(pth.parts) > 1 else Path(".")
            pth = (cfg_dir / pth).resolve()
        return str(pth)

    return _resolve(input_path), _resolve(output_path)


def run_auto_report(input_path: str, config_path: str, output_path: str):
    df = load_csv(input_path)
    # De-duplicate duplicate column names to ensure Series selection works
    if df.columns.duplicated().any():
        print("[report] Duplicate column names detected; de-duplicating.")

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

        df = df.copy()
        df.columns = _make_unique(df.columns)
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
    parser.add_argument(
        "--config-path", default=DEFAULT_CONFIG_PATH, help="Path to report_config CSV"
    )
    parser.add_argument(
        "--input-path",
        default=None,
        help="(Optional) Override input CSV path; if omitted, value from report_config is used",
    )
    parser.add_argument(
        "--output-path",
        default=None,
        help="(Optional) Override output CSV path; if omitted, value from report_config is used",
    )
    parser.add_argument(
        "--no-config-io",
        action="store_true",
        help="If set, do NOT read INPUT/OUTPUT from report_config; use CLI values only",
    )
    args = parser.parse_args()

    # Resolve INPUT/OUTPUT from report_config unless explicitly disabled
    cfg_input, cfg_output = (None, None)
    if not args.no_config_io:
        try:
            cfg_input, cfg_output = read_io_from_config(args.config_path)
        except Exception as e:
            print(f"[config] Warning: could not read INPUT/OUTPUT from config: {e}")

    # Priority: report_config values > CLI overrides > rm defaults to only use config
    input_path = cfg_input or args.input_path
    output_path = cfg_output or args.output_path

    if cfg_input:
        print(f"[config] Using INPUT from report_config: {cfg_input}")
    if cfg_output:
        print(f"[config] Using OUTPUT from report_config: {cfg_output}")

    if not input_path:
        raise SystemExit("INPUT path not provided and not found in report_config.")
    if not output_path:
        raise SystemExit("OUTPUT path not provided and not found in report_config.")

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    run_auto_report(
        input_path=input_path,
        config_path=args.config_path,
        output_path=output_path,
    )
