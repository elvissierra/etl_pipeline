# ETL Pipeline Skeleton

This is a modular, reusable ETL (Extract, Transform, Load) pipeline skeleton using Python and SQLite, designed for quick adaptation in future data processing projects.

---

## 📁 Project Structure
```
ETL_Pipeline/
├── etl/                  # Core modules (extract, transform, load)
├── config/               # Configuration and schema
├── data/                 # Input/output data
├── logs/                 # Logging outputs
├── tests/                # Unit tests (optional)
├── main.py               # Entry point
├── .env                  # Environment variables
└── requirements.txt      # Dependencies
```

---

## ⚙️ Configuration
All parameters can be configured in `config.py` and `.env`.

### Example `.env`
```
DATA_PATH=data/Test_Data.csv
DB_PATH=data/report_data.db
LOG_FILE=etl_errors.log
```

### Example `config.py`
```python
REQUIRED_COLUMNS = ["place_id", "edited_fields", ...]
FILTER_COLUMN = "all_customer_suggested_fields_edited"
FILTER_VALUE = "Yes"
TABLE_NAME = "report_data"
TABLE_SCHEMA = {
    "place_id": "INTEGER PRIMARY KEY",
    "edited_fields": "TEXT",
    ...
}
```

---

## 🚀 Running the Pipeline
```bash
python main.py --input path/to/input.csv --output path/to/output.db
```

If arguments are not provided, defaults from `.env` will be used.

---

## 🧪 Testing
You can add tests using `pytest` in the `tests/` directory to validate transformations and schema logic.

---

## ✅ Features
- Modular `extract`, `transform`, `load` functions
- Configurable schema and column validation
- CLI + `.env` flexibility
- Basic logging with optional log rotation

---

## 📦 Dependencies
```bash
pip install -r requirements.txt
```
Required:
- `pandas`
- `python-dotenv`
```
# requirements.txt
pandas
python-dotenv
```

---

## 📌 License
MIT or your chosen license.

