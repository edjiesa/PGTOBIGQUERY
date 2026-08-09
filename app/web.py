import json
import asyncio
import logging
import threading
from pathlib import Path
from typing import List, Optional, Dict, Any
from contextlib import asynccontextmanager


from fastapi import FastAPI, Request, BackgroundTasks, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from app.config import config, MigrationConfig
from app.extractor import PostgresExtractor
from app.loader import BigQueryLoader
from app.migrator import DatabaseMigrator, active_migration_status
from app.state import state_manager


logger = logging.getLogger("pgtobigquery.web")


main_event_loop: Optional[asyncio.AbstractEventLoop] = None

# Persistent background migration thread tracker
_bg_migration_thread: Optional[threading.Thread] = None
_bg_migration_lock = threading.Lock()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global main_event_loop
    main_event_loop = asyncio.get_running_loop()
    yield


BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

web_app = FastAPI(
    title="PostgreSQL 10.4 to BigQuery Migration Tool",
    description="Web Dashboard for Database Migration to Google BigQuery",
    version="1.0.0",
    lifespan=lifespan
)

# Active WebSocket connections for live logs
active_websockets: List[WebSocket] = []



class ConfigUpdateModel(BaseModel):
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_user: str = "postgres"
    pg_password: str = ""
    pg_database: str = "postgres"
    pg_schema: str = "public"
    pg_sslmode: str = "disable"

    gcp_project_id: Optional[str] = ""
    bigquery_dataset_id: Optional[str] = ""
    gcp_sa_key_json: Optional[str] = None
    batch_size: int = 50000
    write_disposition: str = "WRITE_TRUNCATE"



class MigrationRequestModel(BaseModel):
    tables: Optional[List[str]] = None
    exclude_tables: Optional[List[str]] = None
    dry_run: bool = False


class SyncRecordModel(BaseModel):
    batch_key: str
    timestamp: str
    status: str   # "success" | "partial" | "failed"
    tables_processed: int = 0
    tables_total: int = 0
    errors: List[str] = []
    duration_seconds: float = 0.0



async def broadcast_ws_message(message: Dict[str, Any]):
    """Broadcasts migration status updates to all connected web clients."""
    for ws in list(active_websockets):
        try:
            await ws.send_json(message)
        except Exception:
            if ws in active_websockets:
                active_websockets.remove(ws)


@web_app.get("/health")
@web_app.get("/api/v1/health")
async def health_check():
    """Health check probe endpoint for load balancers and container orchestrators."""
    return {"status": "healthy", "service": "pgtobigquery"}


@web_app.get("/", response_class=HTMLResponse)
async def get_dashboard(request: Request):
    """Renders main dashboard HTML page."""
    # Create a safe copy for the frontend to hide sensitive JSON key
    safe_config = config.model_copy() if hasattr(config, "model_copy") else config.copy()
    if safe_config.gcp_sa_key_json and safe_config.gcp_sa_key_json.strip():
        safe_config.gcp_sa_key_json = "******** (Key is configured)"

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"config": safe_config}
    )



@web_app.get("/api/config")
async def get_config():
    """Returns current environment configuration."""
    masked_key = "******** (Key is configured)" if config.gcp_sa_key_json and config.gcp_sa_key_json.strip() else None
    return {
        "pg_host": config.pg_host,
        "pg_port": config.pg_port,
        "pg_user": config.pg_user,
        "pg_database": config.pg_database,
        "pg_schema": config.pg_schema,
        "pg_sslmode": config.pg_sslmode,
        "gcp_project_id": config.gcp_project_id,
        "bigquery_dataset_id": config.bigquery_dataset_id,
        "gcp_sa_key_json": masked_key,
        "batch_size": config.batch_size,
        "write_disposition": config.write_disposition
    }


@web_app.post("/api/config")
async def update_config(data: ConfigUpdateModel):
    """Updates active config parameters dynamically."""
    
    new_sa_key = data.gcp_sa_key_json
    if new_sa_key == "******** (Key is configured)":
        new_sa_key = config.gcp_sa_key_json
    elif new_sa_key and new_sa_key.strip():
        pass
    else:
        new_sa_key = None
        
    config.update_settings(
        pg_host=data.pg_host,
        pg_port=data.pg_port,
        pg_user=data.pg_user,
        pg_password=data.pg_password,
        pg_database=data.pg_database,
        pg_schema=data.pg_schema,
        pg_sslmode=data.pg_sslmode,
        gcp_project_id=data.gcp_project_id,
        bigquery_dataset_id=data.bigquery_dataset_id,
        gcp_sa_key_json=new_sa_key,
        batch_size=data.batch_size,
        write_disposition=data.write_disposition
    )
    return {"status": "updated", "message": "Configuration & Remote Profile updated successfully."}






def safe_ws_broadcast(message: Dict[str, Any]):
    """Thread-safe WebSocket broadcaster for background worker threads."""
    try:
        loop = main_event_loop or asyncio.get_event_loop()
        if loop and loop.is_running():
            asyncio.run_coroutine_threadsafe(broadcast_ws_message(message), loop)
    except Exception as err:
        logger.warning(f"WebSocket broadcast error: {err}")


async_jobs: Dict[str, Dict[str, Any]] = {}


@web_app.get("/api/job-status/{job_id}")
def get_job_status(job_id: str):
    """Returns status and result of a background job."""
    if job_id in async_jobs:
        return async_jobs[job_id]
    return {"status": "unknown", "message": f"Job '{job_id}' not found."}


@web_app.post("/api/test-postgres")
def test_postgres(background_tasks: BackgroundTasks):
    """Airbyte-style diagnostic connection test for PostgreSQL (Runs off-thread in background if needed)."""
    job_id = "test_postgres"
    async_jobs[job_id] = {"status": "running", "message": "Testing remote PostgreSQL connection..."}

    def run_bg_test():
        try:
            extractor = PostgresExtractor(config)
            res = extractor.test_connection()
            extractor.close()
            async_jobs[job_id] = {"status": "completed", "result": res}
            safe_ws_broadcast({"type": "test_postgres_done", "data": res})
        except Exception as e:
            async_jobs[job_id] = {"status": "failed", "error": str(e)}
            safe_ws_broadcast({"type": "test_postgres_error", "message": str(e)})

    background_tasks.add_task(run_bg_test)
    return {"status": "started", "job_id": job_id, "message": "Diagnostic test started in background."}


@web_app.post("/api/test-bigquery")
def test_bigquery():
    """Airbyte-style diagnostic connection test for Google BigQuery (Runs off-thread)."""
    loader = BigQueryLoader(config)
    res = loader.test_connection()
    return res


@web_app.get("/api/test-connections")
def test_connections():
    """Tests connections to PostgreSQL and BigQuery concurrently (Runs off-thread)."""
    extractor = PostgresExtractor(config)
    pg_res = extractor.test_connection()
    extractor.close()

    loader = BigQueryLoader(config)
    bq_res = loader.test_connection()

    return {
        "postgres": pg_res,
        "bigquery": bq_res
    }


@web_app.get("/api/tables")
def list_tables(background_tasks: BackgroundTasks):
    """Fetches PostgreSQL tables and metadata in background (Optimized for 1000+ slow tables)."""
    cached_tables = state_manager.load_tables()
    if cached_tables:
        return {"status": "success", "tables": cached_tables}

    job_id = "load_tables"
    if job_id in async_jobs and async_jobs[job_id].get("status") == "completed":
        return async_jobs[job_id]["result"]

    async_jobs[job_id] = {"status": "running", "message": "Fetching catalog from remote PostgreSQL..."}

    def run_bg_tables():
        try:
            extractor = PostgresExtractor(config)
            tables = extractor.get_all_tables_metadata()
            extractor.close()
            state_manager.save_tables(tables)
            res = {"status": "success", "tables": tables}
            async_jobs[job_id] = {"status": "completed", "result": res}
            safe_ws_broadcast({"type": "tables_loaded", "count": len(tables)})
        except Exception as e:
            res = {"status": "error", "message": str(e)}
            async_jobs[job_id] = {"status": "failed", "result": res, "error": str(e)}
            safe_ws_broadcast({"type": "tables_error", "message": str(e)})

    background_tasks.add_task(run_bg_tables)
    return {"status": "started", "job_id": job_id, "message": "Loading table catalog in background."}


@web_app.get("/api/table-batches")
def get_table_batches(background_tasks: BackgroundTasks, batch_size: int = 100):
    """
    Groups all PostgreSQL tables into batches of N tables (default 100 per batch),
    sorted ascending by row count (from smallest tables with 0 rows to largest).
    Runs in background to prevent reverse proxy 504 timeouts.
    """
    cached_batches = state_manager.load_batches()
    if cached_batches:
        cached_tables = state_manager.load_tables() or []
        return {
            "status": "success",
            "total_tables": len(cached_tables),
            "total_batches": len(cached_batches),
            "batch_table_size": batch_size,
            "batches": cached_batches
        }

    job_id = "table_batches"
    if job_id in async_jobs and async_jobs[job_id].get("status") == "completed":
        return async_jobs[job_id]["result"]

    async_jobs[job_id] = {"status": "running", "message": "Grouping tables into batches in background..."}

    def run_bg_batches():
        try:
            tables = state_manager.load_tables()
            if not tables:
                extractor = PostgresExtractor(config)
                tables = extractor.get_all_tables_metadata()
                extractor.close()
                state_manager.save_tables(tables)

            tables_sorted = sorted(tables, key=lambda x: x.get("row_count", 0))
            batches = []
            chunk_size = max(1, batch_size)
            total_tables = len(tables_sorted)

            for idx, i in enumerate(range(0, total_tables, chunk_size), start=1):
                chunk = tables_sorted[i:i + chunk_size]
                min_rows = chunk[0]["row_count"] if chunk else 0
                max_rows = chunk[-1]["row_count"] if chunk else 0
                total_rows_chunk = sum(t["row_count"] for t in chunk)
                batch_tables = [t["table_name"] for t in chunk]

                batches.append({
                    "batch_index": idx,
                    "batch_name": f"Kelompok #{idx} ({len(chunk)} Tabel)",
                    "table_count": len(chunk),
                    "min_rows": min_rows,
                    "max_rows": max_rows,
                    "total_rows": total_rows_chunk,
                    "tables": batch_tables,
                    "completed_tables": [],
                    "status": "PENDING",
                    "table_details": chunk
                })

            state_manager.save_batches(batches)
            res = {
                "status": "success",
                "total_tables": total_tables,
                "total_batches": len(batches),
                "batch_table_size": chunk_size,
                "batches": batches
            }
            async_jobs[job_id] = {"status": "completed", "result": res}
            safe_ws_broadcast({"type": "batches_created", "count": len(batches)})
        except Exception as e:
            res = {"status": "error", "message": str(e)}
            async_jobs[job_id] = {"status": "failed", "result": res, "error": str(e)}

    background_tasks.add_task(run_bg_batches)
    return {"status": "started", "job_id": job_id, "message": "Grouping tables in background."}


@web_app.post("/api/migrate")
async def start_migration(req: MigrationRequestModel):
    """Starts migration as a persistent daemon thread - survives browser close."""
    global _bg_migration_thread

    with _bg_migration_lock:
        # Prevent duplicate concurrent migrations
        if _bg_migration_thread and _bg_migration_thread.is_alive():
            return {
                "status": "already_running",
                "message": "Migration is already running in background. Monitor progress from any browser."
            }

    def run_bg_migration():
        def ws_callback(event):
            safe_ws_broadcast(event)
            if isinstance(event, dict) and event.get("event") == "table_success":
                data = event.get("data", {})
                if "table_name" in data:
                    state_manager.update_table_status_in_batches(
                        data["table_name"],
                        status="COMPLETED",
                        rows_migrated=data.get("bigquery_rows", 0)
                    )

        try:
            state_manager.clear_migration_status()
            migrator = DatabaseMigrator(config)
            migrator.run_migration(
                tables=req.tables,
                exclude_tables=req.exclude_tables,
                dry_run=req.dry_run,
                progress_callback=ws_callback
            )
            safe_ws_broadcast({"event": "migration_finished", "data": {"message": "Migrasi selesai!"}})
        except Exception as e:
            logger.error(f"Background migration thread error: {e}", exc_info=True)
            safe_ws_broadcast({
                "event": "table_error",
                "data": {"table_name": "MIGRATION_JOB", "error": f"Migration failed: {str(e)}"}
            })

    with _bg_migration_lock:
        _bg_migration_thread = threading.Thread(
            target=run_bg_migration,
            name="migration-daemon",
            daemon=True   # Dies only when server process dies, NOT when browser closes
        )
        _bg_migration_thread.start()

    return {
        "status": "started",
        "message": "Migration started as background daemon. You can close this browser safely — migration will continue running on the server."
    }


@web_app.get("/api/migration-running")
def is_migration_running():
    """Returns whether a background migration is currently running. Callable from any browser."""
    is_running = _bg_migration_thread is not None and _bg_migration_thread.is_alive()
    persisted = state_manager.load_migration_status()
    progress = active_migration_status if is_running else persisted
    return {
        "is_running": is_running,
        "thread_alive": is_running,
        "progress": progress
    }


@web_app.post("/api/migration-stop")
def stop_migration():
    """Signals the background migration to stop after the current table finishes."""
    active_migration_status["stop_requested"] = True
    return {"status": "stop_requested", "message": "Stop signal sent. Migration will finish the current table then stop."}







@web_app.post("/api/migrate-single")
def migrate_single_table(req: MigrationRequestModel):
    """Synchronously migrates a single target table and returns detailed status/errors immediately."""
    if not req.tables or len(req.tables) == 0:
        return {"status": "failed", "error": "No table specified for single table migration."}

    table_name = req.tables[0]

    try:
        migrator = DatabaseMigrator(config)
        results = migrator.run_migration(tables=[table_name], dry_run=req.dry_run)
        if results.get("errors") and len(results["errors"]) > 0:
            err_msg = results["errors"][0].get("error", "Unknown migration error")
            return {
                "status": "failed",
                "table_name": table_name,
                "error": err_msg,
            }

        table_details = results.get("table_details", [])
        bq_rows = table_details[0].get("bigquery_rows", 0) if table_details else 0
        return {
            "status": "success",
            "table_name": table_name,
            "bigquery_rows": bq_rows,
        }
    except Exception as e:
        logger.error(f"Single table migration error for '{table_name}': {e}", exc_info=True)
        return {
            "status": "failed",
            "table_name": table_name,
            "error": str(e)
        }




@web_app.post("/api/sync-record")
def save_sync_record_endpoint(data: SyncRecordModel):
    """Saves a batch sync record (success/partial/failed + errors) to persistent disk history."""
    record = {
        "timestamp": data.timestamp,
        "status": data.status,
        "tables_processed": data.tables_processed,
        "tables_total": data.tables_total,
        "errors": data.errors,
        "duration_seconds": data.duration_seconds
    }
    state_manager.save_sync_record(data.batch_key, record)
    return {"status": "saved"}


@web_app.get("/api/sync-history")
def get_sync_history_endpoint():
    """Returns full sync history for all batches from persistent disk storage."""
    return state_manager.load_sync_history()


@web_app.get("/api/migration-progress")
def get_migration_progress():

    """Returns real-time migration progress. Merges live in-memory state + persisted disk state so any browser gets full status."""
    is_thread_alive = _bg_migration_thread is not None and _bg_migration_thread.is_alive()

    # Use live in-memory if thread running, else fall back to last persisted status
    if is_thread_alive:
        progress = dict(active_migration_status)
    else:
        progress = state_manager.load_migration_status()
        progress["is_running"] = False

    cached_stats = state_manager.load_stats()
    return {
        "status": "running" if is_thread_alive else "idle",
        "thread_alive": is_thread_alive,
        "progress": progress,
        "saved_stats": cached_stats
    }


@web_app.websocket("/ws/logs")
async def websocket_logs(websocket: WebSocket):
    """WebSocket endpoint for real-time progress & log updates.
    On connect, immediately sends current migration status so reconnecting browsers are caught up.
    """
    await websocket.accept()
    active_websockets.append(websocket)

    # Send current status immediately on connect so reconnecting browsers catch up instantly
    try:
        is_thread_alive = _bg_migration_thread is not None and _bg_migration_thread.is_alive()
        current_progress = dict(active_migration_status) if is_thread_alive else state_manager.load_migration_status()
        current_progress["is_running"] = is_thread_alive
        await websocket.send_json({
            "event": "reconnect_status",
            "data": current_progress
        })
    except Exception:
        pass

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in active_websockets:
            active_websockets.remove(websocket)


