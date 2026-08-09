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
    sanitize_bq_column_name,
    sanitize_bq_table_id,
)

logger = logging.getLogger("pgtobigquery.loader")


class BigQueryLoader:
    def __init__(self, config: MigrationConfig):
        self.config = config
        self._client = None

    @property
    def client(self) -> bigquery.Client:
        """Lazily initializes and returns BigQuery client."""
        if self._client is None:
            self._client = self._init_client()
        return self._client

    def _init_client(self) -> bigquery.Client:
        """Initializes BigQuery Client using configured credentials."""
        project_id = self.config.gcp_project_id

        # 1. From JSON String in env
        if self.config.gcp_sa_key_json:
            info = json.loads(self.config.gcp_sa_key_json)
            creds = service_account.Credentials.from_service_account_info(info)
            return bigquery.Client(project=project_id or creds.project_id, credentials=creds)

        # 2. From Credentials JSON file
        if self.config.gcp_credentials_file and os.path.isfile(self.config.gcp_credentials_file):
            creds = service_account.Credentials.from_service_account_file(self.config.gcp_credentials_file)
            return bigquery.Client(project=project_id or creds.project_id, credentials=creds)

        # 3. Fallback to Application Default Credentials (ADC) or environment variable GOOGLE_APPLICATION_CREDENTIALS
        return bigquery.Client(project=project_id)


    def test_connection(self) -> Dict[str, Any]:
        """
        Performs Airbyte-style step-by-step connection diagnostics for Google BigQuery.
        Returns a detailed checklist of connection tests.
        """
        checklist = [
            {"step": "json_parsing", "name": "Service Account Key Format", "status": "pending", "detail": ""},
            {"step": "authentication", "name": "Google Cloud Authentication", "status": "pending", "detail": ""},
            {"step": "project_check", "name": "GCP Project Access", "status": "pending", "detail": ""},
            {"step": "bigquery_api", "name": "BigQuery API Connectivity", "status": "pending", "detail": ""},
            {"step": "dataset_check", "name": "Target Dataset Permission", "status": "pending", "detail": ""}
        ]

        auth_email = None
        project_id = self.config.gcp_project_id

        # Step 1: Service Account JSON Key Parsing
        try:
            if self.config.gcp_sa_key_json:
                sa_info = json.loads(self.config.gcp_sa_key_json)
                if not isinstance(sa_info, dict) or sa_info.get("type") != "service_account":
                    raise ValueError("JSON Key must be a valid Google Service Account dictionary (type='service_account').")
                auth_email = sa_info.get("client_email", "Unknown SA Email")
                project_id = project_id or sa_info.get("project_id")
                checklist[0]["status"] = "success"
                checklist[0]["detail"] = f"Valid Service Account Key for '{auth_email}'"
            elif self.config.gcp_credentials_file and os.path.isfile(self.config.gcp_credentials_file):
                with open(self.config.gcp_credentials_file, "r") as f:
                    sa_info = json.load(f)

                    auth_email = sa_info.get("client_email", "File Credentials")
                    project_id = project_id or sa_info.get("project_id")
                checklist[0]["status"] = "success"
                checklist[0]["detail"] = f"Loaded file credentials: {self.config.gcp_credentials_file}"
            else:
                checklist[0]["status"] = "warning"
                checklist[0]["detail"] = "Using Application Default Credentials (ADC) or environment settings."
        except Exception as err:
            checklist[0]["status"] = "failed"
            checklist[0]["detail"] = f"JSON Key Error: {str(err)}"
            return {"status": "failed", "message": "Failed Service Account Key Parsing", "checklist": checklist}

        # Step 2 & 3: Authentication & Client Initialization
        try:
            client = self._init_client()
            checklist[1]["status"] = "success"
            checklist[1]["detail"] = f"Authenticated successfully as {auth_email or 'Default GCP Credential'}"

            checklist[2]["status"] = "success"
            checklist[2]["detail"] = f"Accessing GCP Project ID: '{client.project}'"
        except Exception as err:
            checklist[1]["status"] = "failed"
            checklist[1]["detail"] = f"Auth failed: {str(err)}"
            return {"status": "failed", "message": "Google Authentication Failed", "checklist": checklist}

        # Step 4: BigQuery API Connectivity Test
        try:
            datasets = list(client.list_datasets(max_results=10))
            checklist[3]["status"] = "success"
            checklist[3]["detail"] = f"BigQuery API OK. Found {len(datasets)} dataset(s) in project."
        except Exception as err:
            checklist[3]["status"] = "failed"
            checklist[3]["detail"] = f"BigQuery API Error: {str(err)}. Verify BigQuery Admin / Data Viewer permissions."
            return {"status": "failed", "message": "BigQuery API Access Denied", "checklist": checklist}

        # Step 5: Target Dataset Check
        target_dataset = self.config.bigquery_dataset_id
        if target_dataset:
            try:
                dataset_ref = bigquery.DatasetReference(client.project, target_dataset)
                ds = client.get_dataset(dataset_ref)
                checklist[4]["status"] = "success"
                checklist[4]["detail"] = f"Dataset '{target_dataset}' exists in location '{ds.location}'"
            except Exception:
                checklist[4]["status"] = "warning"
                checklist[4]["detail"] = f"Dataset '{target_dataset}' does not exist yet (will be auto-created during migration)."
        else:
            checklist[4]["status"] = "warning"
            checklist[4]["detail"] = "No target Dataset ID configured."

        return {
            "status": "success",
            "message": "All BigQuery connection checks passed!",
            "project_id": client.project,
            "service_account": auth_email,
            "checklist": checklist
        }


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
        """Converts PostgreSQL column schema list to BigQuery SchemaField list with fallback protection."""
        bq_fields = []
        for col in pg_columns:
            name = sanitize_bq_column_name(col["column_name"])
            pg_type = col["pg_type"]
            bq_type, mode = postgres_to_bigquery_type(pg_type)

            field = bigquery.SchemaField(
                name=name,
                field_type=bq_type,
                mode=mode,
                description=f"Migrated from Postgres type: {pg_type}"
            )
            bq_fields.append(field)

        if not bq_fields:
            logger.warning("No columns found in pg_columns schema. Adding fallback 'id' STRING field.")
            bq_fields.append(bigquery.SchemaField("id", "STRING", mode="NULLABLE", description="Fallback schema column"))

        return bq_fields

    def construct_pyarrow_schema(self, pg_columns: List[Dict[str, Any]]) -> pa.Schema:
        """Constructs PyArrow Schema for batch writing with fallback protection."""
        pa_fields = []
        for col in pg_columns:
            name = sanitize_bq_column_name(col["column_name"])
            pg_type = col["pg_type"]
            is_nullable = col.get("is_nullable", True)
            pa_fields.append(postgres_to_pyarrow_field(name, pg_type, is_nullable))

        if not pa_fields:
            pa_fields.append(pa.field("id", pa.string(), nullable=True))

        return pa.schema(pa_fields)

    def write_batch_to_parquet(
        self,
        batch_rows: List[Dict[str, Any]],
        pg_columns: List[Dict[str, Any]],
        output_file_path: str
    ) -> str:
        """Converts batch of dict rows to PyArrow table and writes to Parquet file."""
        if not pg_columns and batch_rows:
            logger.info("Auto-discovering columns directly from batch row keys...")
            pg_columns = [
                {"column_name": k, "pg_type": "varchar", "is_nullable": True}
                for k in batch_rows[0].keys()
            ]

        # 1. Build PyArrow Schema
        pa_schema = self.construct_pyarrow_schema(pg_columns)

        # 2. Build column arrays
        col_data = {}
        for col in pg_columns:
            raw_col_name = col["column_name"]
            sanitized_name = sanitize_bq_column_name(raw_col_name)
            pg_type = col["pg_type"]
            bq_type, mode = postgres_to_bigquery_type(pg_type)

            values = [
                convert_value_for_pyarrow(row.get(raw_col_name), bq_type, mode)
                for row in batch_rows
            ]
            col_data[sanitized_name] = values

        # 3. Build PyArrow Table & write to Parquet
        pa_table = pa.Table.from_pydict(col_data, schema=pa_schema)
        pq.write_table(pa_table, output_file_path, compression="SNAPPY")
        return output_file_path


    def write_all_string_parquet(self, parquet_file_path: str, output_path: str) -> str:
        """Reads existing Parquet file and re-writes all columns as string type."""
        try:
            original = pq.read_table(parquet_file_path)
            str_arrays = {}
            str_fields = []
            for i, col_name in enumerate(original.schema.names):
                arr = original.column(i)
                # Cast every value to string safely
                str_values = [str(v.as_py()) if v.is_valid else None for v in arr]
                str_arrays[col_name] = pa.array(str_values, type=pa.string())
                str_fields.append(pa.field(col_name, pa.string(), nullable=True))
            str_schema = pa.schema(str_fields)
            str_table = pa.Table.from_pydict(str_arrays, schema=str_schema)
            pq.write_table(str_table, output_path, compression="SNAPPY")
            return output_path
        except Exception as e:
            logger.error(f"Failed to re-write Parquet as all-string: {e}")
            raise e

    def load_parquet_to_bigquery(
        self,
        parquet_file_path: str,
        table_id: str,
        bq_schema: List[bigquery.SchemaField],
        write_disposition: str = "WRITE_TRUNCATE"
    ) -> int:
        """Loads a local Parquet file into a BigQuery table using BigQuery Load Job with auto-schema update."""
        dataset_id = self.config.bigquery_dataset_id
        clean_table_id = sanitize_bq_table_id(table_id)
        table_ref = f"{self.client.project}.{dataset_id}.{clean_table_id}"

        # Check existing table schema in BigQuery
        try:
            existing_table = self.client.get_table(table_ref)
            # If existing BigQuery table has fewer columns than PostgreSQL bq_schema or write_disposition is WRITE_TRUNCATE, recreate table schema
            if len(existing_table.schema) < len(bq_schema) and write_disposition == "WRITE_TRUNCATE":
                logger.info(f"Recreating BigQuery table '{clean_table_id}' to expand schema from {len(existing_table.schema)} to {len(bq_schema)} columns...")
                self.client.delete_table(table_ref, not_found_ok=True)
        except Exception:
            pass

        job_config = bigquery.LoadJobConfig(
            schema=bq_schema,
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=write_disposition,
            autodetect=False,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
            ]
        )

        logger.info(f"Starting BigQuery Load Job for table '{clean_table_id}' ({len(bq_schema)} columns)...")
        try:
            with open(parquet_file_path, "rb") as source_file:
                job = self.client.load_table_from_file(source_file, table_ref, job_config=job_config)
            job.result()
            logger.info(f"BigQuery Load Job completed for table '{clean_table_id}'. Output rows: {job.output_rows}")
            return job.output_rows or 0
        except Exception as load_err:
            logger.warning(f"Primary load failed for '{clean_table_id}': {load_err}. Retrying with all-string Parquet fallback...")

            # Fallback: re-write Parquet as all strings, then load with all-STRING BigQuery schema
            fallback_parquet = parquet_file_path + ".str_fallback.parquet"
            try:
                self.write_all_string_parquet(parquet_file_path, fallback_parquet)
                self.client.delete_table(table_ref, not_found_ok=True)
                string_schema = [
                    bigquery.SchemaField(f.name, "STRING", mode="NULLABLE", description=f.description)
                    for f in bq_schema
                ]
                fallback_job_config = bigquery.LoadJobConfig(
                    schema=string_schema,
                    source_format=bigquery.SourceFormat.PARQUET,
                    write_disposition="WRITE_TRUNCATE",
                    autodetect=False
                )
                with open(fallback_parquet, "rb") as source_file:
                    fallback_job = self.client.load_table_from_file(source_file, table_ref, job_config=fallback_job_config)
                fallback_job.result()
                logger.info(f"Fallback Load Job completed for '{clean_table_id}'. Output rows: {fallback_job.output_rows}")
                return fallback_job.output_rows or 0
            except Exception as final_err:
                logger.error(f"Both primary and fallback load jobs failed for '{clean_table_id}': {final_err}")
                raise final_err
            finally:
                if os.path.exists(fallback_parquet):
                    os.remove(fallback_parquet)




    def create_empty_table_if_not_exists(
        self,
        table_id: str,
        bq_schema: List[bigquery.SchemaField]
    ) -> bigquery.Table:
        """Creates an empty table with specified schema in BigQuery if it does not exist (or updates schema if outdated)."""
        dataset_id = self.config.bigquery_dataset_id
        clean_table_id = sanitize_bq_table_id(table_id)
        table_ref = f"{self.client.project}.{dataset_id}.{clean_table_id}"

        try:
            table = self.client.get_table(table_ref)
            if len(table.schema) < len(bq_schema):
                logger.info(f"Recreating outdated BigQuery empty table '{clean_table_id}' ({len(table.schema)} -> {len(bq_schema)} columns)...")
                self.client.delete_table(table_ref, not_found_ok=True)
                table = bigquery.Table(table_ref, schema=bq_schema)
                table = self.client.create_table(table, exists_ok=True)
            else:
                logger.info(f"BigQuery table '{clean_table_id}' already exists with {len(table.schema)} columns.")
            return table
        except Exception:
            logger.info(f"Creating empty BigQuery table '{clean_table_id}' with {len(bq_schema)} columns...")
            table = bigquery.Table(table_ref, schema=bq_schema)
            table = self.client.create_table(table, exists_ok=True)
            logger.info(f"Successfully created empty BigQuery table '{clean_table_id}'.")
            return table


    def get_table_row_count(self, table_id: str) -> int:
        """Gets actual row count of a table directly from BigQuery metadata/query."""
        dataset_id = self.config.bigquery_dataset_id
        clean_table_id = sanitize_bq_table_id(table_id)
        table_ref = f"{self.client.project}.{dataset_id}.{clean_table_id}"
        try:
            table = self.client.get_table(table_ref)
            return table.num_rows or 0
        except Exception as e:
            logger.warning(f"Could not fetch BigQuery row count for '{clean_table_id}': {e}")
            return 0


