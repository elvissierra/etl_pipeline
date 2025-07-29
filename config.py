from dotenv import load_dotenv
import os 

load_dotenv()


DB_HOST = os.getenv("DB_HOST", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_USER = os.getenv("DB_USER", "")
DB_PORT = os.getenv("DB_PORT", "")
DATA_PATH = os.getenv("DATA_PATH", "data/Test_Data.csv")
DB_PATH = os.getenv("DB_PATH", "data/report_data.db")
LOG_FILE = os.getenv("LOG_FILE", "etl_errors.log")