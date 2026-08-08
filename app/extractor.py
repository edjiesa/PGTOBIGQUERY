import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, List, Any, Generator, Tuple
from app.config import MigrationConfig

logger = logging.getLogger("pgtobigquery.extractor")


class PostgresExtractor:
    def __init__(self, config: MigrationConfig):
        self.config = config
        self.conn = None

    def connect(self):
        """Establishes database connection to PostgreSQL 10.4."""
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(
                host=self.config.pg_host,
                port=self.config.pg_port,
                user=self.config.pg_user,
                password=self.config.pg_password,
                dbname=self.config.pg_database,
                connect_timeout=10
            )
            logger.info(f"Connected to PostgreSQL at {self.config.pg_host}:{self.config.pg_port}/{self.config.pg_database}")

    def close(self):
        """Closes PostgreSQL database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Closed PostgreSQL database connection.")

    def test_connection(self) -> Dict[str, Any]:
        """
        Performs Airbyte-style step-by-step connection diagnostics for PostgreSQL 10.4.
        Returns a detailed checklist of connection tests.
        """
        checklist = [
            {"step": "tcp_handshake", "name": "PostgreSQL Host Reachable", "status": "pending", "detail": ""},
            {"step": "authentication", "name": "User Authentication", "status": "pending", "detail": ""},
            {"step": "version_check", "name": "PostgreSQL Version & Catalog", "status": "pending", "detail": ""},
            {"step": "catalog_permission", "name": "Schema & Table Inspection", "status": "pending", "detail": ""}
        ]

        try:
            self.connect()
            checklist[0]["status"] = "success"
            checklist[0]["detail"] = f"Connected to {self.config.pg_host}:{self.config.pg_port}"

            checklist[1]["status"] = "success"
            checklist[1]["detail"] = f"Authenticated as user '{self.config.pg_user}' on db '{self.config.pg_database}'"

            with self.conn.cursor() as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()[0]
                checklist[2]["status"] = "success"
                checklist[2]["detail"] = f"{version.split(',')[0]}"

                tables = self.get_tables()
                checklist[3]["status"] = "success"
                checklist[3]["detail"] = f"Schema '{self.config.pg_schema}' contains {len(tables)} base table(s)."

            return {
                "status": "success",
                "message": "PostgreSQL connection test succeeded!",
                "database": self.config.pg_database,
                "schema": self.config.pg_schema,
                "table_count": len(tables),
                "checklist": checklist
            }
        except Exception as e:
            checklist[0]["status"] = "failed"
            checklist[0]["detail"] = str(e)
            return {
                "status": "failed",
                "message": f"PostgreSQL connection failed: {str(e)}",
                "checklist": checklist
            }


    def get_tables(self, schema: str = None) -> List[str]:
        """Retrieves list of user base tables in the specified PostgreSQL schema."""
        self.connect()
        target_schema = schema or self.config.pg_schema
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (target_schema,))
            tables = [row[0] for row in cur.fetchall()]
        return tables

    def get_table_schema(self, table_name: str, schema: str = None) -> List[Dict[str, Any]]:
        """
        Extracts column definitions, PostgreSQL data types, and nullability for a table.
        Querying catalog views compatible with PostgreSQL 10.4.
        """
        self.connect()
        target_schema = schema or self.config.pg_schema
        query = """
            SELECT 
                column_name,
                data_type,
                udt_name,
                is_nullable,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (target_schema, table_name))
            columns = cur.fetchall()

        result = []
        for col in columns:
            col_name = col["column_name"]
            data_type = col["data_type"]
            udt_name = col["udt_name"]
            is_nullable = col["is_nullable"].upper() == "YES"

            # Check if it's an ARRAY type or user-defined udt
            if data_type == "ARRAY":
                pg_type = f"{udt_name}[]"
            elif data_type == "USER-DEFINED":
                pg_type = udt_name
            else:
                pg_type = data_type

            result.append({
                "column_name": col_name,
                "pg_type": pg_type,
                "is_nullable": is_nullable,
                "position": col["ordinal_position"]
            })

        return result

    def get_row_count(self, table_name: str, schema: str = None) -> int:
        """Gets exact total row count for a table."""
        self.connect()
        target_schema = schema or self.config.pg_schema
        query = f'SELECT COUNT(*) FROM "{target_schema}"."{table_name}";'
        with self.conn.cursor() as cur:
            cur.execute(query)
            count = cur.fetchone()[0]
        return count

    def stream_table_data(self, table_name: str, batch_size: int = 50000, schema: str = None) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Streams rows from PostgreSQL using a named server-side cursor to save memory.
        Yields list of dict rows for each batch.
        """
        self.connect()
        target_schema = schema or self.config.pg_schema
        cursor_name = f"stream_{target_schema}_{table_name}".replace("-", "_")

        # Named cursor for server-side streaming
        with self.conn.cursor(name=cursor_name, cursor_factory=RealDictCursor) as stream_cur:
            stream_cur.itersize = batch_size
            query = f'SELECT * FROM "{target_schema}"."{table_name}";'
            stream_cur.execute(query)

            while True:
                rows = stream_cur.fetchmany(batch_size)
                if not rows:
                    break
                yield rows
