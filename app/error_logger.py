import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Any

logger = logging.getLogger("pgtobigquery.error_logger")

ERROR_LOG_FILE = os.environ.get("DATA_DIR", "/app/data") + "/migration_errors.json"

def log_migration_error(table_name: str, error_msg: str):
    """
    Appends a new error record to the persistent JSON error log file.
    Creates the file if it does not exist.
    """
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(ERROR_LOG_FILE), exist_ok=True)
        
        errors = []
        if os.path.exists(ERROR_LOG_FILE):
            with open(ERROR_LOG_FILE, "r") as f:
                try:
                    errors = json.load(f)
                except json.JSONDecodeError:
                    errors = []

        # Add new error at the beginning (newest first)
        new_error = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "table_name": table_name,
            "error": str(error_msg)
        }
        errors.insert(0, new_error)
        
        # Limit to last 500 errors to prevent file bloat
        errors = errors[:500]

        with open(ERROR_LOG_FILE, "w") as f:
            json.dump(errors, f, indent=2)
            
    except Exception as e:
        logger.error(f"Failed to write to error log file: {e}")

def get_migration_errors() -> List[Dict[str, Any]]:
    """
    Retrieves the list of logged errors.
    """
    if os.path.exists(ERROR_LOG_FILE):
        try:
            with open(ERROR_LOG_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to read error log file: {e}")
    return []

def clear_migration_errors() -> bool:
    """
    Clears the error log file.
    """
    try:
        with open(ERROR_LOG_FILE, "w") as f:
            json.dump([], f)
        return True
    except Exception as e:
        logger.error(f"Failed to clear error log file: {e}")
        return False
