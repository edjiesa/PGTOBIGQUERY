import json
import asyncio
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, Request, BackgroundTasks, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from app.config import config, MigrationConfig
from app.extractor import PostgresExtractor
from app.loader import BigQueryLoader
from app.migrator import DatabaseMigrator

logger = logging.getLogger("pgtobigquery.web")

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

web_app = FastAPI(
    title="PostgreSQL 10.4 to BigQuery Migration Tool",
    description="Web Dashboard for Database Migration to Google BigQuery",
    version="1.0.0"
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
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"config": config}
    )



@web_app.get("/api/config")
async def get_config():
    """Returns current environment configuration."""
    return {
        "pg_host": config.pg_host,
        "pg_port": config.pg_port,
        "pg_user": config.pg_user,
        "pg_database": config.pg_database,
        "pg_schema": config.pg_schema,
        "pg_sslmode": config.pg_sslmode,
        "gcp_project_id": config.gcp_project_id,
        "bigquery_dataset_id": config.bigquery_dataset_id,
        "gcp_sa_key_json": config.gcp_sa_key_json,
        "batch_size": config.batch_size,
        "write_disposition": config.write_disposition
    }


@web_app.post("/api/config")
async def update_config(data: ConfigUpdateModel):
    """Updates active config parameters dynamically."""
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
        gcp_sa_key_json=data.gcp_sa_key_json if data.gcp_sa_key_json and data.gcp_sa_key_json.strip() else None,
        batch_size=data.batch_size,
        write_disposition=data.write_disposition
    )
    return {"status": "updated", "message": "Configuration & Remote Profile updated successfully."}



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
            # Broadcast over WebSocket
            asyncio.run(broadcast_ws_message({"type": "test_postgres_done", "data": res}))
        except Exception as e:
            async_jobs[job_id] = {"status": "failed", "error": str(e)}
            asyncio.run(broadcast_ws_message({"type": "test_postgres_error", "message": str(e)}))

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
    job_id = "load_tables"
    
    # If job is already completed recently, return result immediately
    if job_id in async_jobs and async_jobs[job_id].get("status") == "completed":
        return async_jobs[job_id]["result"]

    async_jobs[job_id] = {"status": "running", "message": "Fetching catalog from remote PostgreSQL..."}

    def run_bg_tables():
        try:
            extractor = PostgresExtractor(config)
            tables = extractor.get_all_tables_metadata()
            extractor.close()
            res = {"status": "success", "tables": tables}
            async_jobs[job_id] = {"status": "completed", "result": res}
            asyncio.run(broadcast_ws_message({"type": "tables_loaded", "count": len(tables)}))
        except Exception as e:
            res = {"status": "error", "message": str(e)}
            async_jobs[job_id] = {"status": "failed", "result": res, "error": str(e)}
            asyncio.run(broadcast_ws_message({"type": "tables_error", "message": str(e)}))

    background_tasks.add_task(run_bg_tables)
    return {"status": "started", "job_id": job_id, "message": "Loading table catalog in background."}





@web_app.get("/api/table-batches")
def get_table_batches(batch_size: int = 100):
    """
    Groups all PostgreSQL tables into batches of N tables (default 100 per batch),
    sorted ascending by row count (from smallest tables with 0 rows to largest).
    """
    try:
        extractor = PostgresExtractor(config)
        tables = extractor.get_all_tables_metadata()
        extractor.close()

        # Sort tables by row_count ASCENDING (smallest to largest)
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
                "table_details": chunk
            })

        return {
            "status": "success",
            "total_tables": total_tables,
            "total_batches": len(batches),
            "batch_table_size": chunk_size,
            "batches": batches
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


@web_app.post("/api/migrate")

async def start_migration(req: MigrationRequestModel, background_tasks: BackgroundTasks):
    """Starts migration task in background."""
    def run_bg_migration():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        migrator = DatabaseMigrator(config)

        def ws_callback(event):
            loop.run_until_complete(broadcast_ws_message(event))

        migrator.run_migration(
            tables=req.tables,
            exclude_tables=req.exclude_tables,
            dry_run=req.dry_run,
            progress_callback=ws_callback
        )
        loop.close()

    background_tasks.add_task(run_bg_migration)
    return {"status": "started", "message": "Migration process started in background."}


@web_app.websocket("/ws/logs")
async def websocket_logs(websocket: WebSocket):
    """WebSocket endpoint for real-time progress & log updates."""
    await websocket.accept()
    active_websockets.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in active_websockets:
            active_websockets.remove(websocket)
