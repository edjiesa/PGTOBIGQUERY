import os
import tempfile
import time
import threading
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Callable, Optional
from app.config import MigrationConfig
from app.extractor import PostgresExtractor
from app.loader import BigQueryLoader
from app.type_mapper import postgres_to_bigquery_type, sanitize_bq_table_id

logger = logging.getLogger("pgtobigquery.migrator")

# Default parallel workers - enough to saturate BigQuery API without overwhelming PG
DEFAULT_WORKERS = 5

active_migration_status: Dict[str, Any] = {
    "is_running": False,
    "total_tables": 0,
    "tables_processed": 0,
    "total_rows_migrated": 0,
    "current_table_name": "-",
    "current_table_index": 0,
    "current_table_rows": 0,
    "current_table_loaded_rows": 0,
    "last_completed_table": None,
    "last_completed_rows": 0,
    "logs": []
}

_status_lock = threading.Lock()


def _update_status(**kwargs):
    """Thread-safe update to active_migration_status."""
    with _status_lock:
        for k, v in kwargs.items():
            active_migration_status[k] = v


def _increment_status(field: str, amount: int = 1):
    """Thread-safe increment of a numeric field in active_migration_status."""
    with _status_lock:
        active_migration_status[field] = active_migration_status.get(field, 0) + amount


class DatabaseMigrator:
    def __init__(self, config: MigrationConfig):
        self.config = config
        # Shared loader for BigQuery (thread-safe client)
        self.loader = BigQueryLoader(config)
        # Shared extractor for catalog-level operations only
        self.extractor = PostgresExtractor(config)

    def _migrate_single_table(
        self,
        table_name: str,
        table_idx: int,
        total_tables: int,
        dry_run: bool,
        notify_progress: Callable
    ) -> Dict[str, Any]:
        """
        Migrates a single table using its own dedicated PostgreSQL connection.
        Safe to run in parallel threads.
        """
        table_start_time = time.time()

        # Each thread gets its own extractor (own PG connection)
        worker_extractor = PostgresExtractor(self.config)

        try:
            pg_cols = worker_extractor.get_table_schema(table_name)
            bq_schema = self.loader.construct_bq_schema(pg_cols)
            row_count = worker_extractor.get_row_count(table_name)

            table_info = {
                "table_name": table_name,
                "columns_count": len(pg_cols),
                "postgres_rows": row_count,
                "bigquery_rows": 0,
                "status": "pending",
                "duration_seconds": 0.0,
                "schema_mapping": [
                    {"col": c["column_name"], "pg": c["pg_type"], "bq": postgres_to_bigquery_type(c["pg_type"])[0]}
                    for c in pg_cols
                ]
            }

            _update_status(current_table_name=table_name, current_table_index=table_idx, current_table_rows=row_count)

            notify_progress("table_start", {
                "table_index": table_idx,
                "total_tables": total_tables,
                "table_name": table_name,
                "row_count": row_count
            })

            if dry_run:
                table_info["status"] = "dry_run_success"
                table_info["duration_seconds"] = round(time.time() - table_start_time, 2)
                return table_info

            # Stream data and load to BigQuery
            rows_loaded = 0
            batch_num = 0

            stream = worker_extractor.stream_table_data(table_name, batch_size=self.config.batch_size)

            for batch in stream:
                batch_num += 1
                write_disp = self.config.write_disposition if batch_num == 1 else "WRITE_APPEND"

                with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
                    tmp_path = tmp_file.name

                try:
                    self.loader.write_batch_to_parquet(batch, pg_cols, tmp_path)
                    self.loader.load_parquet_to_bigquery(
                        parquet_file_path=tmp_path,
                        table_id=table_name,
                        bq_schema=bq_schema,
                        write_disposition=write_disp
                    )
                    rows_loaded += len(batch)
                finally:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)

                notify_progress("batch_complete", {
                    "table_name": table_name,
                    "batch_num": batch_num,
                    "rows_batch": len(batch),
                    "total_rows_loaded": rows_loaded,
                    "total_expected": row_count
                })

            # Empty table — create schema in BigQuery
            if batch_num == 0:
                self.loader.create_empty_table_if_not_exists(table_name, bq_schema)

            # Verify BigQuery row count
            bq_verified_rows = self.loader.get_table_row_count(table_name)
            final_bq_rows = bq_verified_rows if bq_verified_rows > 0 else rows_loaded

            table_info["bigquery_rows"] = final_bq_rows
            table_info["status"] = "success"
            table_info["duration_seconds"] = round(time.time() - table_start_time, 2)
            table_info["bq_table_ref"] = (
                f"{self.loader.client.project}.{self.config.bigquery_dataset_id}."
                f"{sanitize_bq_table_id(table_name)}"
            )

            _increment_status("tables_processed")
            _increment_status("total_rows_migrated", final_bq_rows)
            _update_status(last_completed_table=table_name, last_completed_rows=final_bq_rows)

            notify_progress("table_success", table_info)
            return table_info

        except Exception as table_err:
            logger.error(f"Error migrating table '{table_name}': {table_err}", exc_info=True)
            err_info = {
                "table_name": table_name,
                "status": "failed",
                "error": str(table_err),
                "duration_seconds": round(time.time() - table_start_time, 2)
            }
            _increment_status("tables_processed")
            with _status_lock:
                active_migration_status["last_error_table"] = table_name
                active_migration_status["last_error_msg"] = str(table_err)
                active_migration_status["logs"].append(
                    f"✗ [MIGRATION ERROR] Table '{table_name}' failed: {table_err}"
                )
            notify_progress("table_error", err_info)
            return err_info

        finally:
            worker_extractor.close()

    def run_migration(
        self,
        tables: Optional[List[str]] = None,
        exclude_tables: Optional[List[str]] = None,
        dry_run: bool = False,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        max_workers: int = DEFAULT_WORKERS
    ) -> Dict[str, Any]:
        """
        Executes parallel database migration from PostgreSQL to Google BigQuery.
        Uses ThreadPoolExecutor to process multiple tables concurrently.
        """
        global active_migration_status
        start_time = time.time()
        results = {
            "status": "completed",
            "tables_processed": 0,
            "total_rows_migrated": 0,
            "table_details": [],
            "errors": []
        }

        notify_lock = threading.Lock()

        def notify_progress(event_type: str, data: Dict[str, Any]):
            if progress_callback:
                with notify_lock:
                    progress_callback({"event": event_type, "data": data})

        try:
            # 1. Ensure BigQuery dataset exists
            if not dry_run:
                self.loader.ensure_dataset_exists()

            # 2. Resolve target tables list
            if tables:
                target_tables = list(tables)
            else:
                target_tables = self.extractor.get_tables()

            if exclude_tables:
                target_tables = [t for t in target_tables if t not in exclude_tables]

            total_tables = len(target_tables)

            with _status_lock:
                active_migration_status["is_running"] = True
                active_migration_status["total_tables"] = total_tables
                active_migration_status["tables_processed"] = 0
                active_migration_status["total_rows_migrated"] = 0
                active_migration_status["logs"] = []

            notify_progress("start", {
                "total_tables": total_tables,
                "tables": target_tables,
                "dry_run": dry_run
            })

            # 3. Parallel table migration
            # Use fewer workers for single-table requests (no overhead)
            effective_workers = 1 if total_tables == 1 else min(max_workers, total_tables)
            logger.info(f"Starting parallel migration of {total_tables} tables with {effective_workers} workers...")

            table_results: Dict[str, Dict] = {}

            with ThreadPoolExecutor(max_workers=effective_workers, thread_name_prefix="migrator") as executor:
                future_to_table = {
                    executor.submit(
                        self._migrate_single_table,
                        table_name,
                        idx,
                        total_tables,
                        dry_run,
                        notify_progress
                    ): table_name
                    for idx, table_name in enumerate(target_tables, start=1)
                }

                for future in as_completed(future_to_table):
                    table_name = future_to_table[future]
                    try:
                        result = future.result()
                        table_results[table_name] = result
                    except Exception as exc:
                        logger.error(f"Unexpected future error for '{table_name}': {exc}")
                        table_results[table_name] = {
                            "table_name": table_name,
                            "status": "failed",
                            "error": str(exc)
                        }

            # 4. Merge results in original table order
            for table_name in target_tables:
                result = table_results.get(table_name, {"table_name": table_name, "status": "unknown"})
                results["table_details"].append(result)
                if result.get("status") == "success":
                    results["tables_processed"] += 1
                    results["total_rows_migrated"] += result.get("bigquery_rows", 0)
                elif result.get("status") in ("failed", "unknown"):
                    results["errors"].append(result)

            # 5. Finish
            results["total_duration_seconds"] = round(time.time() - start_time, 2)
            active_migration_status["is_running"] = False
            notify_progress("finish", results)
            return results

        finally:
            active_migration_status["is_running"] = False
            try:
                self.extractor.close()
            except Exception:
                pass
