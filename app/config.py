import os
from typing import Optional, List
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MigrationConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    # PostgreSQL Connection Settings
    pg_host: str = Field(default="localhost", alias="PG_HOST")
    pg_port: int = Field(default=5432, alias="PG_PORT")
    pg_user: str = Field(default="postgres", alias="PG_USER")
    pg_password: str = Field(default="postgres", alias="PG_PASSWORD")
    pg_database: str = Field(default="postgres", alias="PG_DATABASE")
    pg_schema: str = Field(default="public", alias="PG_SCHEMA")
    pg_sslmode: str = Field(default="prefer", alias="PG_SSLMODE")

    # BigQuery Settings
    gcp_project_id: Optional[str] = Field(default=None, alias="GCP_PROJECT_ID")
    bigquery_dataset_id: Optional[str] = Field(default=None, alias="BIGQUERY_DATASET_ID")
    gcp_credentials_file: Optional[str] = Field(default=None, alias="GCP_CREDENTIALS_FILE")
    gcp_sa_key_json: Optional[str] = Field(default=None, alias="GCP_SA_KEY_JSON")
    bigquery_location: str = Field(default="US", alias="BIGQUERY_LOCATION")

    # Engine Settings
    batch_size: int = Field(default=50000, alias="BATCH_SIZE")
    write_disposition: str = Field(default="WRITE_TRUNCATE", alias="WRITE_DISPOSITION")
    auto_create_tables: bool = Field(default=True, alias="AUTO_CREATE_TABLES")
    use_parquet: bool = Field(default=True, alias="USE_PARQUET")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    # Server Settings
    web_host: str = Field(default="0.0.0.0", alias="WEB_HOST")
    web_port: int = Field(default=8000, alias="WEB_PORT")

    def get_pg_connection_string(self) -> str:
        """Returns standard PostgreSQL connection URI string with sslmode."""
        return f"postgresql://{self.pg_user}:{self.pg_password}@{self.pg_host}:{self.pg_port}/{self.pg_database}?sslmode={self.pg_sslmode}"

    def update_settings(self, **kwargs):
        """Dynamically update settings fields at runtime."""
        for key, value in kwargs.items():
            if hasattr(self, key) and value is not None:
                setattr(self, key, value)



# Single instance singleton loader
config = MigrationConfig()
