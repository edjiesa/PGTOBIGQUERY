import pytest
from app.config import MigrationConfig
from app.extractor import PostgresExtractor


def test_config_defaults():
    cfg = MigrationConfig(
        PG_HOST="localhost",
        PG_PORT=5432,
        PG_USER="postgres",
        PG_PASSWORD="secretpassword",
        PG_DATABASE="testdb"
    )
    assert cfg.get_pg_connection_string() == "postgresql://postgres:secretpassword@localhost:5432/testdb"


def test_extractor_init():
    cfg = MigrationConfig()
    extractor = PostgresExtractor(cfg)
    assert extractor.conn is None
