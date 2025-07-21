import os 

DATA_PATH = os.getenv("DATA_PATH", "data/Test_Data.csv")
DB_PATH = os.getenv("DB_PATH", "data/report_data.db")
LOG_FILE = os.getenv("LOG_FILE", "etl_errors.log")