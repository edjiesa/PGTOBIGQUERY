import os
import sys
import typer
import logging
from typing import List, Optional
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.panel import Panel
from rich.live import Live

from app.config import config
from app.migrator import DatabaseMigrator
from app.extractor import PostgresExtractor
from app.loader import BigQueryLoader

app = typer.Typer(
    name="pgtobigquery",
    help="PostgreSQL 10.4 to Google BigQuery Migration Tool",
    add_completion=False
)
console = Console()


@app.command("test-connection")
def test_connection():
    """Test connections to PostgreSQL database and Google BigQuery."""
    console.print(Panel.fit("[bold blue]PostgreSQL 10.4 -> BigQuery Connection Test[/bold blue]"))

    # Test Postgres
    extractor = PostgresExtractor(config)
    pg_res = extractor.test_connection()
    extractor.close()

    if pg_res["status"] == "success":
        console.print(f"[bold green]✓ PostgreSQL Connected successfully![/bold green]")
        console.print(f"  [dim]Version: {pg_res['version']}[/dim]")
        console.print(f"  [dim]Database: {pg_res['database']} | Schema: {pg_res['schema']}[/dim]\n")
    else:
        console.print(f"[bold red]✗ PostgreSQL Connection Failed:[/bold red] {pg_res['message']}\n")

    # Test BigQuery
    loader = BigQueryLoader(config)
    bq_res = loader.test_connection()
    if bq_res["status"] == "success":
        console.print(f"[bold green]✓ BigQuery Connected successfully![/bold green]")
        console.print(f"  [dim]Project ID: {bq_res['project_id']}[/dim]\n")
    else:
        console.print(f"[bold red]✗ BigQuery Connection Failed:[/bold red] {bq_res['message']}\n")


@app.command("schema-preview")
def schema_preview(
    schema: str = typer.Option("public", help="PostgreSQL schema name"),
    table: Optional[str] = typer.Option(None, help="Target specific table name")
):
    """Preview PostgreSQL table columns and their mapped BigQuery types."""
    extractor = PostgresExtractor(config)
    tables = [table] if table else extractor.get_tables(schema=schema)

    if not tables:
        console.print(f"[yellow]No tables found in schema '{schema}'.[/yellow]")
        extractor.close()
        return

    for t in tables:
        cols = extractor.get_table_schema(t, schema=schema)
        row_cnt = extractor.get_row_count(t, schema=schema)

        tbl = Table(title=f"Table: {schema}.{t} ({row_cnt:,} rows)")
        tbl.add_column("Pos", style="cyan", justify="right")
        tbl.add_column("Column Name", style="bold white")
        tbl.add_column("PostgreSQL Type", style="magenta")
        tbl.add_column("BigQuery Type", style="green")
        tbl.add_column("Nullable", style="yellow")

        for c in cols:
            from app.type_mapper import postgres_to_bigquery_type
            bq_type, mode = postgres_to_bigquery_type(c["pg_type"])
            tbl.add_row(
                str(c["position"]),
                c["column_name"],
                c["pg_type"],
                f"{bq_type} ({mode})",
                "YES" if c["is_nullable"] else "NO"
            )

        console.print(tbl)
        console.print("")

    extractor.close()


@app.command("migrate")
def migrate(
    tables: Optional[List[str]] = typer.Option(None, "--table", "-t", help="Specific table(s) to migrate"),
    exclude: Optional[List[str]] = typer.Option(None, "--exclude", "-e", help="Table(s) to exclude"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Simulate schema mapping without writing data"),
    batch_size: int = typer.Option(50000, "--batch-size", "-b", help="Row batch size per parquet load")
):
    """Run database migration from PostgreSQL 10.4 to Google BigQuery."""
    if batch_size:
        config.batch_size = batch_size

    console.print(Panel.fit("[bold green]Starting PostgreSQL 10.4 to BigQuery Migration[/bold green]"))

    migrator = DatabaseMigrator(config)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console
    ) as progress:

        main_task = progress.add_task("Migrating Database...", total=100)

        def on_progress(event):
            evt_type = event["event"]
            data = event["data"]

            if evt_type == "start":
                progress.update(main_task, total=data["total_tables"], completed=0)
                console.print(f"[cyan]Found {data['total_tables']} tables to migrate.[/cyan]")

            elif evt_type == "table_start":
                progress.update(main_task, description=f"Processing table: [bold]{data['table_name']}[/bold] ({data['row_count']:,} rows)")

            elif evt_type == "table_success":
                progress.advance(main_task, 1)
                console.print(f"[green]✓ Table '{data['table_name']}' migrated successfully! ({data['postgres_rows']:,} rows in {data['duration_seconds']}s)[/green]")

            elif evt_type == "table_error":
                progress.advance(main_task, 1)
                console.print(f"[red]✗ Table '{data['table_name']}' failed: {data['error']}[/red]")

        res = migrator.run_migration(
            tables=tables,
            exclude_tables=exclude,
            dry_run=dry_run,
            progress_callback=on_progress
        )

    console.print("\n[bold green]Migration Summary:[/bold green]")
    console.print(f"  • Tables Processed: {res['tables_processed']}")
    console.print(f"  • Total Rows Migrated: {res['total_rows_migrated']:,}")
    console.print(f"  • Total Time: {res.get('total_duration_seconds', 0)} seconds")
    if res["errors"]:
        console.print(f"  • [red]Errors: {len(res['errors'])}[/red]")


@app.command("serve")
def serve(
    host: str = typer.Option("0.0.0.0", help="Host address for Web UI"),
    port: int = typer.Option(8000, help="Port for Web UI")
):
    """Launch FastAPI Web UI Dashboard."""
    import uvicorn
    console.print(f"[bold cyan]Launching Web Dashboard at http://{host}:{port}...[/bold cyan]")
    uvicorn.run("app.web:web_app", host=host, port=port, reload=False)


def main():
    app()


if __name__ == "__main__":
    main()
