# config/config.py
import os
from pathlib import Path
from dotenv import load_dotenv
from etl.utils import load_yaml

# 1) Load .env for secrets overrides (DB passwords, etc)
load_dotenv(Path(__file__).parent / ".env")

# 2) Pick a base YAML based on ENV var
env = os.getenv("PIPELINE_ENV", "dev")
_base = load_yaml(Path(__file__).parent / f"{env}.yaml")

# 3) Expose config constants
DATA_PATH    = _base["extract"]["data_path"]
DB_PATH      = _base["load"]["db_path"]
LOG_FILE     = _base["logging"]["log_file"]

REQUIRED_COLUMNS = _base["transform"]["required_columns"]
FILTER_COLUMN    = _base["transform"]["filter_col"]
FILTER_VALUE     = _base["transform"]["filter_val"]

TABLE_NAME   = _base["load"]["table_name"]
TABLE_SCHEMA = _base["load"]["table_schema"]