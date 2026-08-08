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
    pg_host: str
    pg_port: int
    pg_user: str
    pg_password: str
    pg_database: str
    pg_schema: str = "public"
    pg_sslmode: str = "prefer"
    gcp_project_id: str
    bigquery_dataset_id: str
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


@web_app.get("/", response_class=HTMLResponse)
async def get_dashboard(request: Request):
    """Renders main dashboard HTML page."""
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "config": config}
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



@web_app.post("/api/test-postgres")
async def test_postgres():
    """Airbyte-style diagnostic connection test for PostgreSQL."""
    extractor = PostgresExtractor(config)
    res = extractor.test_connection()
    extractor.close()
    return res


@web_app.post("/api/test-bigquery")
async def test_bigquery():
    """Airbyte-style diagnostic connection test for Google BigQuery."""
    loader = BigQueryLoader(config)
    res = loader.test_connection()
    return res


@web_app.get("/api/test-connections")
async def test_connections():
    """Tests connections to PostgreSQL and BigQuery concurrently."""
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
async def list_tables():
    """Fetches PostgreSQL tables and metadata."""
    try:
        extractor = PostgresExtractor(config)
        tables = extractor.get_tables()

        result = []
        for t in tables:
            cnt = extractor.get_row_count(t)
            cols = extractor.get_table_schema(t)
            result.append({
                "table_name": t,
                "row_count": cnt,
                "column_count": len(cols),
                "columns": cols
            })
        extractor.close()
        return {"status": "success", "tables": result}
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
