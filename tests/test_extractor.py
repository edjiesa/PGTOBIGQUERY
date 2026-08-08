import pytest
from app.config import MigrationConfig
from app.extractor import PostgresExtractor


def test_config_defaults():
    cfg = MigrationConfig(
        PG_HOST="db.remote-example.com",
        PG_PORT=5432,
        PG_USER="postgres",
        PG_PASSWORD="secretpassword",
        PG_DATABASE="remotedb",
        PG_SSLMODE="require"
    )
    assert cfg.get_pg_connection_string() == "postgresql://postgres:secretpassword@db.remote-example.com:5432/remotedb?sslmode=require"


def test_extractor_init():
    cfg = MigrationConfig()
    extractor = PostgresExtractor(cfg)
    assert extractor.conn is None
