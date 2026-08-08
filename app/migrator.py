import os
import tempfile
import time
import logging
from typing import List, Dict, Any, Callable, Optional
from app.config import MigrationConfig
from app.extractor import PostgresExtractor
from app.loader import BigQueryLoader
from app.type_mapper import postgres_to_bigquery_type

logger = logging.getLogger("pgtobigquery.migrator")


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


class DatabaseMigrator:
    def __init__(self, config: MigrationConfig):
        self.config = config
        self.extractor = PostgresExtractor(config)
        self.loader = BigQueryLoader(config)

    def run_migration(
        self,
        tables: Optional[List[str]] = None,
        exclude_tables: Optional[List[str]] = None,
        dry_run: bool = False,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None
    ) -> Dict[str, Any]:
        """
        Executes database migration from PostgreSQL 10.4 to Google BigQuery.
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

        def notify_progress(event_type: str, data: Dict[str, Any]):
            if progress_callback:
                progress_callback({"event": event_type, "data": data})

        try:
            # 1. Ensure BigQuery dataset exists (if not dry run)
            if not dry_run:
                self.loader.ensure_dataset_exists()

            # 2. Get list of tables
            if tables:
                target_tables = list(tables)
            else:
                target_tables = self.extractor.get_tables()

            if exclude_tables:
                target_tables = [t for t in target_tables if t not in exclude_tables]


            active_migration_status["is_running"] = True
            active_migration_status["total_tables"] = len(target_tables)
            active_migration_status["tables_processed"] = 0
            active_migration_status["total_rows_migrated"] = 0

            notify_progress("start", {
                "total_tables": len(target_tables),
                "tables": target_tables,
                "dry_run": dry_run
            })


            # 3. Process each table
            for table_idx, table_name in enumerate(target_tables, start=1):
                table_start_time = time.time()
                try:
                    # Get table schema & row count
                    pg_cols = self.extractor.get_table_schema(table_name)
                    bq_schema = self.loader.construct_bq_schema(pg_cols)
                    row_count = self.extractor.get_row_count(table_name)

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

                    active_migration_status["current_table_name"] = table_name
                    active_migration_status["current_table_index"] = table_idx
                    active_migration_status["current_table_rows"] = row_count
                    active_migration_status["current_table_loaded_rows"] = 0

                    notify_progress("table_start", {
                        "table_index": table_idx,
                        "total_tables": len(target_tables),
                        "table_name": table_name,
                        "row_count": row_count
                    })

                    if dry_run:
                        table_info["status"] = "dry_run_success"
                        table_info["duration_seconds"] = round(time.time() - table_start_time, 2)
                        results["table_details"].append(table_info)
                        results["tables_processed"] += 1
                        continue

                    # Migration execution via streaming chunks & Parquet loading
                    rows_loaded = 0
                    batch_num = 0
                    stream = self.extractor.stream_table_data(
                        table_name,
                        batch_size=self.config.batch_size
                    )

                    for batch in stream:
                        batch_num += 1
                        write_disp = self.config.write_disposition if batch_num == 1 else "WRITE_APPEND"

                        # Create temp parquet file
                        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
                            tmp_path = tmp_file.name

                        try:
                            self.loader.write_batch_to_parquet(batch, pg_cols, tmp_path)
                            loaded = self.loader.load_parquet_to_bigquery(
                                parquet_file_path=tmp_path,
                                table_id=table_name,
                                bq_schema=bq_schema,
                                write_disposition=write_disp
                            )
                            rows_loaded += len(batch)
                            active_migration_status["current_table_loaded_rows"] = rows_loaded
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

                    # If table has 0 rows or yielded no batches, create empty table schema in BigQuery
                    if batch_num == 0:
                        self.loader.create_empty_table_if_not_exists(table_name, bq_schema)

                    # Verify actual rows present in BigQuery
                    bq_verified_rows = self.loader.get_table_row_count(table_name)
                    final_bq_rows = bq_verified_rows if bq_verified_rows > 0 else rows_loaded

                    table_info["bigquery_rows"] = final_bq_rows
                    table_info["status"] = "success"
                    table_info["duration_seconds"] = round(time.time() - table_start_time, 2)
                    table_info["bq_table_ref"] = f"{self.loader.client.project}.{self.config.bigquery_dataset_id}.{table_name}"
                    results["tables_processed"] += 1
                    results["total_rows_migrated"] += final_bq_rows
                    results["table_details"].append(table_info)

                    active_migration_status["tables_processed"] += 1
                    active_migration_status["total_rows_migrated"] += final_bq_rows
                    active_migration_status["last_completed_table"] = table_name
                    active_migration_status["last_completed_rows"] = final_bq_rows

                    notify_progress("table_success", table_info)


                except Exception as table_err:
                    logger.error(f"Error migrating table '{table_name}': {table_err}", exc_info=True)
                    table_err_info = {
                        "table_name": table_name,
                        "status": "failed",
                        "error": str(table_err)
                    }
                    results["errors"].append(table_err_info)
                    results["table_details"].append(table_err_info)
                    active_migration_status["tables_processed"] += 1
                    active_migration_status["last_completed_table"] = table_name
                    active_migration_status["last_completed_rows"] = 0
                    notify_progress("table_error", table_err_info)


            # 4. Finish migration run
            results["total_duration_seconds"] = round(time.time() - start_time, 2)
            active_migration_status["is_running"] = False
            notify_progress("finish", results)
            return results
        finally:
            active_migration_status["is_running"] = False
            self.extractor.close()
