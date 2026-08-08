import os
import tempfile
import json
import logging
from typing import List, Dict, Any
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery
from google.oauth2 import service_account

from app.config import MigrationConfig
from app.type_mapper import (
    postgres_to_bigquery_type,
    postgres_to_pyarrow_field,
    convert_value_for_pyarrow,
)

logger = logging.getLogger("pgtobigquery.loader")


class BigQueryLoader:
    def __init__(self, config: MigrationConfig):
        self.config = config
        self.client = self._init_client()

    def _init_client(self) -> bigquery.Client:
        """Initializes BigQuery Client using configured credentials."""
        project_id = self.config.gcp_project_id

        # 1. From JSON String in env
        if self.config.gcp_sa_key_json:
            info = json.loads(self.config.gcp_sa_key_json)
            creds = service_account.Credentials.from_service_account_info(info)
            return bigquery.Client(project=project_id or creds.project_id, credentials=creds)

        # 2. From Credentials JSON file
        if self.config.gcp_credentials_file and os.path.exists(self.config.gcp_credentials_file):
            creds = service_account.Credentials.from_service_account_file(self.config.gcp_credentials_file)
            return bigquery.Client(project=project_id or creds.project_id, credentials=creds)

        # 3. Fallback to Application Default Credentials (ADC) or environment variable GOOGLE_APPLICATION_CREDENTIALS
        return bigquery.Client(project=project_id)

    def test_connection(self) -> Dict[str, Any]:
        """Tests connection to BigQuery and verifies project accessibility."""
        try:
            datasets = list(self.client.list_datasets(max_results=5))
            return {
                "status": "success",
                "project_id": self.client.project,
                "dataset_count": len(datasets)
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def ensure_dataset_exists(self, dataset_id: str = None, location: str = None) -> bigquery.Dataset:
        """Creates dataset if it does not exist."""
        target_dataset = dataset_id or self.config.bigquery_dataset_id
        if not target_dataset:
            raise ValueError("BigQuery Dataset ID is required.")

        dataset_ref = bigquery.DatasetReference(self.client.project, target_dataset)
        try:
            dataset = self.client.get_dataset(dataset_ref)
            logger.info(f"Dataset '{target_dataset}' already exists.")
            return dataset
        except Exception:
            logger.info(f"Dataset '{target_dataset}' not found. Creating dataset...")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = location or self.config.bigquery_location
            dataset = self.client.create_dataset(dataset, timeout=30)
            logger.info(f"Successfully created dataset '{target_dataset}'.")
            return dataset

    def construct_bq_schema(self, pg_columns: List[Dict[str, Any]]) -> List[bigquery.SchemaField]:
        """Converts PostgreSQL column schema list to BigQuery SchemaField list."""
        bq_fields = []
        for col in pg_columns:
            name = col["column_name"]
            pg_type = col["pg_type"]
            bq_type, mode = postgres_to_bigquery_type(pg_type)

            field = bigquery.SchemaField(
                name=name,
                field_type=bq_type,
                mode=mode,
                description=f"Migrated from Postgres type: {pg_type}"
            )
            bq_fields.append(field)
        return bq_fields

    def construct_pyarrow_schema(self, pg_columns: List[Dict[str, Any]]) -> pa.Schema:
        """Constructs PyArrow Schema for batch writing."""
        pa_fields = []
        for col in pg_columns:
            name = col["column_name"]
            pg_type = col["pg_type"]
            is_nullable = col.get("is_nullable", True)
            pa_fields.append(postgres_to_pyarrow_field(name, pg_type, is_nullable))
        return pa.schema(pa_fields)

    def write_batch_to_parquet(
        self,
        batch_rows: List[Dict[str, Any]],
        pg_columns: List[Dict[str, Any]],
        output_file_path: str
    ) -> str:
        """Converts batch of dict rows to PyArrow table and writes to Parquet file."""
        # 1. Build PyArrow Schema
        pa_schema = self.construct_pyarrow_schema(pg_columns)

        # 2. Build column arrays
        col_data = {}
        for col in pg_columns:
            col_name = col["column_name"]
            pg_type = col["pg_type"]
            bq_type, mode = postgres_to_bigquery_type(pg_type)

            values = [
                convert_value_for_pyarrow(row.get(col_name), bq_type, mode)
                for row in batch_rows
            ]
            col_data[col_name] = values

        # 3. Build PyArrow Table & write to Parquet
        pa_table = pa.Table.from_pydict(col_data, schema=pa_schema)
        pq.write_table(pa_table, output_file_path, compression="SNAPPY")
        return output_file_path

    def load_parquet_to_bigquery(
        self,
        parquet_file_path: str,
        table_id: str,
        bq_schema: List[bigquery.SchemaField],
        write_disposition: str = "WRITE_TRUNCATE"
    ) -> int:
        """Loads a local Parquet file into a BigQuery table using BigQuery Load Job."""
        dataset_id = self.config.bigquery_dataset_id
        table_ref = f"{self.client.project}.{dataset_id}.{table_id}"

        job_config = bigquery.LoadJobConfig(
            schema=bq_schema,
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=write_disposition,
            autodetect=False
        )

        logger.info(f"Starting BigQuery Load Job for table '{table_id}' from {parquet_file_path}...")
        with open(parquet_file_path, "rb") as source_file:
            job = self.client.load_table_from_file(source_file, table_ref, job_config=job_config)

        job.result()  # Wait for job completion
        logger.info(f"BigQuery Load Job completed for table '{table_id}'. Output rows: {job.output_rows}")
        return job.output_rows or 0
