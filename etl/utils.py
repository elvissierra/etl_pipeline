

import json
from pathlib import Path
import yaml

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