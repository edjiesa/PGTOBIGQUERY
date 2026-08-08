import socket
import logging
import psycopg2
import concurrent.futures
from psycopg2.extras import RealDictCursor
from typing import Dict, List, Any, Generator, Tuple
from app.config import MigrationConfig

logger = logging.getLogger("pgtobigquery.extractor")


class PostgresExtractor:
    def __init__(self, config: MigrationConfig):
        self.config = config
        self.conn = None

    def connect(self):
        """Establishes database connection to PostgreSQL / EnterpriseDB 10.x (Remote or Local)."""
        if self.conn is not None and not self.conn.closed:
            try:
                with self.conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                return
            except Exception:
                self.close()

        conn_kwargs = {
            "host": self.config.pg_host,
            "port": self.config.pg_port,
            "user": self.config.pg_user,
            "password": self.config.pg_password,
            "dbname": self.config.pg_database,
            "sslmode": self.config.pg_sslmode or "disable",
            "connect_timeout": 60
        }

        try:
            self.conn = psycopg2.connect(**conn_kwargs, gssencmode="disable")
        except Exception:
            self.conn = psycopg2.connect(**conn_kwargs)

        logger.info(f"Connected to PostgreSQL at {self.config.pg_host}:{self.config.pg_port}/{self.config.pg_database} (sslmode={self.config.pg_sslmode})")


    def close(self):
        """Closes PostgreSQL database connection."""
        if self.conn and not self.conn.closed:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None
            logger.info("Closed PostgreSQL database connection.")

    def test_connection(self) -> Dict[str, Any]:
        """
        Performs Airbyte-style step-by-step connection diagnostics for PostgreSQL / EnterpriseDB 10.x.
        Extended timeout support for large databases with 1000+ tables.
        """
        self.close()
        checklist = [
            {"step": "tcp_handshake", "name": "PostgreSQL Host Reachable", "status": "pending", "detail": ""},
            {"step": "authentication", "name": "User Authentication", "status": "pending", "detail": ""},
            {"step": "version_check", "name": "PostgreSQL Version & Catalog", "status": "pending", "detail": ""},
            {"step": "catalog_permission", "name": "Schema & Table Inspection", "status": "pending", "detail": ""}
        ]

        try:
            # Step 1: TCP Handshake Check (10s Timeout)
            try:
                sock = socket.create_connection((self.config.pg_host, self.config.pg_port), timeout=10.0)
                sock.close()
                checklist[0]["status"] = "success"
                checklist[0]["detail"] = f"Reachable {self.config.pg_host}:{self.config.pg_port}"
            except Exception as socket_err:
                checklist[0]["status"] = "failed"
                checklist[0]["detail"] = f"Host '{self.config.pg_host}:{self.config.pg_port}' is unreachable ({socket_err}). Check IP address, port, firewall rules, or VPN."
                return {
                    "status": "failed",
                    "message": f"PostgreSQL host unreachable: {socket_err}",
                    "checklist": checklist
                }

            # Step 2: Authentication Test
            try:
                self.connect()
                with self.conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                checklist[1]["status"] = "success"
                checklist[1]["detail"] = f"Authenticated as user '{self.config.pg_user}' on db '{self.config.pg_database}'"
            except Exception as auth_err:
                checklist[1]["status"] = "failed"
                checklist[1]["detail"] = f"Authentication failed: {str(auth_err)}"
                return {
                    "status": "failed",
                    "message": f"User Authentication failed: {str(auth_err)}",
                    "checklist": checklist
                }



            # Step 3: Version Check
            try:
                with self.conn.cursor() as cur:
                    cur.execute("SELECT version();")
                    version = cur.fetchone()[0]
                checklist[2]["status"] = "success"
                checklist[2]["detail"] = f"{version.split(',')[0]}"
            except Exception as ver_err:
                checklist[2]["status"] = "warning"
                checklist[2]["detail"] = f"Could not read version: {str(ver_err)}"

            # Step 4: Catalog & Table Inspection (Ultra-fast 1ms pg_class query)
            table_count = 0
            try:
                with self.conn.cursor() as cur:
                    try:
                        cur.execute("""
                            SELECT COUNT(*)
                            FROM pg_class c
                            JOIN pg_namespace n ON n.oid = c.relnamespace
                            WHERE n.nspname = %s AND c.relkind = 'r';
                        """, (self.config.pg_schema,))
                        table_count = cur.fetchone()[0]
                    except Exception:
                        self.conn.rollback()
                        cur.execute("SELECT COUNT(*) FROM pg_tables WHERE schemaname = %s;", (self.config.pg_schema,))
                        table_count = cur.fetchone()[0]

                checklist[3]["status"] = "success"
                checklist[3]["detail"] = f"Schema '{self.config.pg_schema}' contains {table_count} base table(s)."
            except Exception as cat_err:
                checklist[3]["status"] = "warning"
                checklist[3]["detail"] = f"Catalog query warning: {str(cat_err)}"

            return {
                "status": "success",
                "message": "PostgreSQL connection test succeeded!",
                "database": self.config.pg_database,
                "schema": self.config.pg_schema,
                "table_count": table_count,
                "checklist": checklist
            }

        finally:
            self.close()


    def get_all_tables_metadata(self, schema: str = None) -> List[Dict[str, Any]]:
        """
        Retrieves all table names, estimated row counts, and column counts in 1 SINGLE BULK QUERY.
        Optimized for large databases with 1000+ tables.
        """
        self.connect()
        target_schema = schema or self.config.pg_schema

        safe_schema = target_schema.replace("'", "''")
        query = f"""
            SELECT 
                c.relname AS table_name,
                GREATEST(c.reltuples::bigint, 0) AS row_count,
                c.relnatts AS column_count
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = '{safe_schema}'
              AND c.relkind = 'r'
            ORDER BY c.relname;
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(query)
                rows = cur.fetchall()


                result = []
                for r in rows:
                    result.append({
                        "table_name": r["table_name"],
                        "row_count": int(r["row_count"]),
                        "column_count": int(r["column_count"]),
                        "columns": []
                    })
                return result
            except Exception as e:
                logger.warning(f"Bulk catalog query failed ({e}), falling back to get_tables")
                self.conn.rollback()
                tables = self.get_tables(target_schema)
                return [{"table_name": t, "row_count": 0, "column_count": 0, "columns": []} for t in tables]

    def get_tables(self, schema: str = None) -> List[str]:

        """Retrieves list of user base tables in the specified PostgreSQL / EnterpriseDB schema."""
        self.connect()
        target_schema = schema or self.config.pg_schema
        with self.conn.cursor() as cur:
            try:
                # Fast pg_tables query compatible with EnterpriseDB and Postgres
                cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = %s ORDER BY tablename;", (target_schema,))
                tables = [row[0] for row in cur.fetchall()]
            except Exception:
                self.conn.rollback()
                query = """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s
                      AND table_type = 'BASE TABLE'
                    ORDER BY table_name;
                """
                cur.execute(query, (target_schema,))
                tables = [row[0] for row in cur.fetchall()]
        return tables


    def get_table_schema(self, table_name: str, schema: str = None) -> List[Dict[str, Any]]:
        """
        Extracts ALL column definitions, PostgreSQL data types, and nullability for a table.
        Queries pg_attribute catalog directly with clean transaction rollbacks & OID resolution.
        """
        self.connect()
        target_schema = schema or self.config.pg_schema

        columns = []

        # Attempt 1: Direct system catalog (pg_attribute) - EDB & Postgres compatible
        try:
            self.conn.rollback()
            query_pg_catalog = """
                SELECT 
                    a.attname AS column_name,
                    format_type(a.atttypid, a.atttypmod) AS data_type,
                    t.typname AS udt_name,
                    NOT a.attnotnull AS is_nullable,
                    a.attnum AS ordinal_position
                FROM pg_attribute a
                JOIN pg_class c ON c.oid = a.attrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_type t ON t.oid = a.atttypid
                WHERE n.nspname = %s 
                  AND LOWER(c.relname) = LOWER(%s)
                  AND a.attnum > 0 
                  AND NOT a.attisdropped
                ORDER BY a.attnum;
            """
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query_pg_catalog, (target_schema, table_name))
                columns = cur.fetchall()
        except Exception as e:
            logger.warning(f"pg_attribute catalog query failed for '{table_name}': {e}")
            try:
                self.conn.rollback()
            except Exception:
                pass

        # Attempt 2: Fallback to information_schema.columns (case-insensitive)
        if not columns:
            try:
                self.conn.rollback()
                query_inf_schema = """
                    SELECT 
                        column_name,
                        data_type,
                        udt_name,
                        is_nullable,
                        ordinal_position
                    FROM information_schema.columns
                    WHERE table_schema = %s AND LOWER(table_name) = LOWER(%s)
                    ORDER BY ordinal_position;
                """
                with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query_inf_schema, (target_schema, table_name))
                    columns = cur.fetchall()
            except Exception as e:
                logger.warning(f"information_schema query failed for '{table_name}': {e}")
                try:
                    self.conn.rollback()
                except Exception:
                    pass

        # Attempt 3: Ultimate Fallback - SELECT * FROM table LIMIT 0 (psycopg2 description + bulk OID format)
        if not columns:
            try:
                self.conn.rollback()
                with self.conn.cursor() as cur:
                    cur.execute(f'SELECT * FROM "{target_schema}"."{table_name}" WHERE 1=0;')
                    if cur.description:
                        columns = []
                        oids = [desc[1] for desc in cur.description if desc[1]]
                        oid_type_map = {}
                        if oids:
                            try:
                                self.conn.rollback()
                                with self.conn.cursor() as oid_cur:
                                    oid_cur.execute("SELECT oid, typname FROM pg_type;")
                                    for r in oid_cur.fetchall():
                                        oid_type_map[r[0]] = r[1]

                            except Exception as oid_err:
                                logger.warning(f"Could not map type OIDs for table '{table_name}': {oid_err}")
                                try:
                                    self.conn.rollback()
                                except Exception:
                                    pass

                        for idx, desc in enumerate(cur.description, start=1):
                            col_name = desc[0]
                            type_oid = desc[1]
                            type_name = oid_type_map.get(type_oid, "varchar")
                            columns.append({
                                "column_name": col_name,
                                "data_type": type_name,
                                "udt_name": type_name,
                                "is_nullable": True,
                                "ordinal_position": idx
                            })
            except Exception as desc_err:
                logger.error(f"Fallback SELECT * failed for table '{table_name}': {desc_err}")
                try:
                    self.conn.rollback()
                except Exception:
                    pass


        result = []
        for col in columns:
            col_name = str(col["column_name"])
            data_type = str(col["data_type"])
            udt_name = str(col["udt_name"])
            is_null = col.get("is_nullable")
            is_nullable = is_null if isinstance(is_null, bool) else (str(is_null).upper() in ("YES", "TRUE", "1"))

            clean_type = data_type.lower()
            if "character varying" in clean_type or "varchar" in clean_type:
                pg_type = "varchar"
            elif "timestamp" in clean_type:
                pg_type = "timestamp"
            elif "numeric" in clean_type or "decimal" in clean_type:
                pg_type = "numeric"
            elif "integer" in clean_type or "bigint" in clean_type or "smallint" in clean_type:
                pg_type = clean_type.split("(")[0]
            elif clean_type == "array" or udt_name.startswith("_"):
                pg_type = f"{udt_name.lstrip('_')}[]"
            elif clean_type == "user-defined":
                pg_type = udt_name
            else:
                pg_type = clean_type.split("(")[0]

            result.append({
                "column_name": col_name,
                "pg_type": pg_type,
                "is_nullable": is_nullable,
                "position": col.get("ordinal_position", len(result) + 1)
            })

        logger.info(f"Extracted {len(result)} columns for table '{table_name}'")
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
