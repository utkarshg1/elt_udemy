import json
from datetime import date
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def load_data():
    data_dir = Path("data")
    file_name = f"YT_data_{date.today()}.json"
    data_path = data_dir / file_name
    try:
        logger.info(f"Processing file from path : {data_path}")
        with open(data_path, "r", encoding="utf-8") as raw_data:
            data = json.load(raw_data)
        return data
    except FileNotFoundError:
        logger.error(f"File not found at : {data_path}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in file : {data_path}")
        raise
