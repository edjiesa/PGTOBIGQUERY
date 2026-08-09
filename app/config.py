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
    pg_sslmode: str = Field(default="disable", alias="PG_SSLMODE")

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
        """Dynamically update settings fields at runtime and persist to .env file."""
        env_lines = []
        env_path = ".env"
        
        # Read existing .env if it exists and is a file
        if os.path.exists(env_path):
            if os.path.isdir(env_path):
                import logging
                logging.getLogger(__name__).warning(f"'{env_path}' is a directory, not a file. Docker likely created it automatically. Cannot persist config.")
            else:
                try:
                    with open(env_path, "r", encoding="utf-8") as f:
                        env_lines = f.readlines()
                except Exception as e:
                    import logging
                    logging.getLogger(__name__).warning(f"Failed to read '{env_path}': {e}")
                
        env_dict = {}
        for line in env_lines:
            if "=" in line and not line.strip().startswith("#"):
                k, v = line.split("=", 1)
                env_dict[k.strip()] = v.strip()

        # Update in memory and prepare for .env
        for key, value in kwargs.items():
            if hasattr(self, key) and value is not None:
                setattr(self, key, value)
                
                # Find the env alias for the field
                field_info = self.model_fields.get(key)
                if field_info and field_info.alias:
                    env_key = field_info.alias
                else:
                    env_key = key.upper()
                
                # Handle JSON string formatting for .env
                if isinstance(value, str) and "\n" in value:
                    # Replace actual newlines with literal \n for env string
                    value = value.replace("\n", "\\n")
                
                env_dict[env_key] = str(value)

        # Write back to .env if it's not a directory
        if not os.path.isdir(env_path):
            try:
                with open(env_path, "w", encoding="utf-8") as f:
                    for k, v in env_dict.items():
                        f.write(f"{k}={v}\n")
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning(f"Failed to write to '{env_path}': {e}")



# Single instance singleton loader
config = MigrationConfig()
