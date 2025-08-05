import json
from pathlib import Path
import yaml
import os
import time
import functools
import logging
import sqlite3
from contextlib import contextmanager


def load_json(path: str) -> dict:
    """
    Load a JSON file from the given path and return its contents as a dict.
    """
    file = Path(path)
    if not file.is_file():
        raise FileNotFoundError(f"JSON config file not found: {path}")
    return json.loads(file.read_text())

def load_yaml(path: str) -> dict:
    """
    Load a YAML file from the given path and return its contents as a dict.
    """
    file = Path(path)
    if not file.is_file():
        raise FileNotFoundError(f"YAML config file not found: {path}")
    return yaml.safe_load(file.read_text())

def ensure_dir(path: str):
    """
    Ensure the parent directory of the given path exists.
    """
    d = Path(path).parent
    d.mkdir(parents=True, exist_ok=True)

def write_df(df, path: str, index: bool = False):
    """
    Write a DataFrame to CSV after ensuring the directory exists.
    """
    ensure_dir(path)
    df.to_csv(path, index=index)

def timeit(func):
    """
    Decorator to log the execution time of functions.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        logging.getLogger(func.__module__).info(
            "%s took %.2f seconds", func.__name__, elapsed
        )
        return result
    return wrapper

def retry(exceptions, tries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """
    Decorator to retry a function if specified exceptions occur.
    """
    def decorator(f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except exceptions:
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)
        return wrapped
    return decorator

@contextmanager
def db_connection(db_path: str):
    """
    Context manager for SQLite database connections.
    """
    conn = sqlite3.connect(db_path)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()