import os
import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional

logger = logging.getLogger("pgtobigquery.state")

# Define persistent storage directory (Defaults to /app/data inside Docker container)
DATA_DIR = Path(os.getenv("DATA_DIR", Path(__file__).resolve().parent.parent / "data"))

class StateManager:
    def __init__(self, data_dir: Path = DATA_DIR):
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.tables_file = self.data_dir / "tables.json"
        self.batches_file = self.data_dir / "batches.json"
        self.profiles_file = self.data_dir / "profiles.json"
        self.stats_file = self.data_dir / "stats.json"
        self.migration_status_file = self.data_dir / "migration_status.json"

    def save_tables(self, tables: List[Dict[str, Any]]) -> bool:
        """Saves raw table metadata to persistent disk storage."""
        try:
            with open(self.tables_file, "w", encoding="utf-8") as f:
                json.dump(tables, f, indent=2)
            logger.info(f"Saved {len(tables)} tables to persistent storage: {self.tables_file}")
            return True
        except Exception as e:
            logger.error(f"Error saving tables to persistent storage: {e}")
            return False

    def load_tables(self) -> Optional[List[Dict[str, Any]]]:
        """Loads table metadata from persistent disk storage if present."""
        if not self.tables_file.exists():
            return None
        try:
            with open(self.tables_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list) and len(data) > 0:
                    return data
        except Exception as e:
            logger.error(f"Error loading tables from persistent storage: {e}")
        return None

    def save_batches(self, batches: List[Dict[str, Any]]) -> bool:
        """Saves grouped table batches and completion statuses to persistent disk storage."""
        try:
            with open(self.batches_file, "w", encoding="utf-8") as f:
                json.dump(batches, f, indent=2)
            logger.info(f"Saved {len(batches)} table batches to persistent storage: {self.batches_file}")
            return True
        except Exception as e:
            logger.error(f"Error saving batches to persistent storage: {e}")
            return False

    def load_batches(self) -> Optional[List[Dict[str, Any]]]:
        """Loads grouped table batches from persistent disk storage if present."""
        if not self.batches_file.exists():
            return None
        try:
            with open(self.batches_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list) and len(data) > 0:
                    return data
        except Exception as e:
            logger.error(f"Error loading batches from persistent storage: {e}")
        return None

    def update_table_status_in_batches(self, table_name: str, status: str = "COMPLETED", rows_migrated: int = 0):
        """Updates completion status for a specific table across batch records."""
        batches = self.load_batches()
        if not batches:
            return

        updated = False
        for b in batches:
            if "tables" in b and table_name in b["tables"]:
                if "completed_tables" not in b or not isinstance(b["completed_tables"], list):
                    b["completed_tables"] = []
                if table_name not in b["completed_tables"]:
                    b["completed_tables"].append(table_name)
                    updated = True

                # Check if all tables in batch are completed
                if len(b["completed_tables"]) >= b.get("table_count", len(b["tables"])):
                    b["status"] = "COMPLETED"
                    updated = True

        if updated:
            self.save_batches(batches)

    def save_stats(self, stats: Dict[str, Any]) -> bool:
        """Saves overall migration progress stats to persistent storage."""
        try:
            with open(self.stats_file, "w", encoding="utf-8") as f:
                json.dump(stats, f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Error saving stats to persistent storage: {e}")
            return False

    def load_stats(self) -> Dict[str, Any]:
        """Loads overall migration progress stats from persistent storage."""
        if not self.stats_file.exists():
            return {"total_tables": 0, "tables_processed": 0, "total_rows_migrated": 0}
        try:
            with open(self.stats_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading stats from persistent storage: {e}")
            return {"total_tables": 0, "tables_processed": 0, "total_rows_migrated": 0}


    def save_migration_status(self, status: Dict[str, Any]) -> bool:
        """Saves full migration progress status to persistent disk so any browser can read it."""
        try:
            with open(self.migration_status_file, "w", encoding="utf-8") as f:
                json.dump(status, f, indent=2, default=str)
            return True
        except Exception as e:
            logger.error(f"Error saving migration status: {e}")
            return False

    def load_migration_status(self) -> Dict[str, Any]:
        """Loads last persisted migration progress status from disk."""
        if not self.migration_status_file.exists():
            return {"is_running": False, "tables_processed": 0, "total_tables": 0}
        try:
            with open(self.migration_status_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading migration status: {e}")
            return {"is_running": False, "tables_processed": 0, "total_tables": 0}

    def clear_migration_status(self):
        """Clears persisted migration status on new migration start."""
        try:
            if self.migration_status_file.exists():
                self.migration_status_file.unlink()
        except Exception:
            pass


# Singleton instance
state_manager = StateManager()
