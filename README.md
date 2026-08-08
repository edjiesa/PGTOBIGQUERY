# 🚀 PostgreSQL 10.4 to Google BigQuery Migration Tool (Dockerized)

![PostgreSQL to BigQuery](https://img.shields.io/badge/PostgreSQL-10.4-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![Google BigQuery](https://img.shields.io/badge/Google_BigQuery-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white)
![Docker Compose](https://img.shields.io/badge/Docker_Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Python 3.11+](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)

A high-performance, containerized database migration tool designed specifically to migrate schema and data from **PostgreSQL 10.4** to **Google BigQuery**. 

Features an **Apache Parquet streaming engine**, a **Rich CLI interface**, and a **Modern Web Dashboard (Web UI)** with real-time migration logs over WebSockets.

---

## 🌟 Key Features

1. **PostgreSQL 10.4 Type Compatibility**:
   - Explicitly inspects PostgreSQL 10 catalog metadata (`information_schema` & `pg_catalog`).
   - Accurately converts Postgres 10 data types (`integer`, `bigint`, `numeric`, `double precision`, `boolean`, `json`, `jsonb`, `uuid`, `timestamp with/without timezone`, `date`, `bytea`, and `ARRAY` types like `text[]`).
2. **High-Speed Parquet Data Pipeline**:
   - Uses PostgreSQL server-side cursors (`FETCH N`) to stream data with minimal memory footprint.
   - Converts batches into in-memory/temporary Apache Parquet files (`pyarrow`).
   - Ingests data directly using Google BigQuery's high-speed Parquet Load API (`load_table_from_file`).
3. **Flexible Execution Modes**:
   - **Dry-Run Mode**: Inspect mapped BigQuery schemas and row counts without altering destination tables.
   - **Write Disposition**: Supports `WRITE_TRUNCATE` (replace table), `WRITE_APPEND` (append rows), or `WRITE_EMPTY`.
   - Table inclusion/exclusion filters.
4. **Dual User Interface**:
   - **Rich Terminal CLI**: Interactive colored progress bars, tables, and status screens.
   - **Modern Web Dashboard**: Glassmorphism UI built with FastAPI + HTML5/CSS3 with live WebSocket progress logs.
5. **Docker & Docker-Compose Ready**:
   - Includes `Dockerfile` and `docker-compose.yml` bundled with a `postgres:10.4-alpine` test container preloaded with test sample data.

---

## 📊 PostgreSQL 10.4 to BigQuery Data Type Mapping

| PostgreSQL 10.4 Type | BigQuery Data Type | BigQuery Mode |
| :--- | :--- | :--- |
| `smallint`, `integer`, `bigint`, `serial`, `bigserial`, `oid` | `INT64` | `NULLABLE` |
| `real`, `double precision` | `FLOAT64` | `NULLABLE` |
| `numeric`, `decimal` | `NUMERIC` | `NULLABLE` |
| `boolean` | `BOOL` | `NULLABLE` |
| `date` | `DATE` | `NULLABLE` |
| `timestamp without time zone` | `DATETIME` | `NULLABLE` |
| `timestamp with time zone` | `TIMESTAMP` | `NULLABLE` |
| `time` | `TIME` | `NULLABLE` |
| `json`, `jsonb` | `JSON` | `NULLABLE` |
| `bytea` | `BYTES` | `NULLABLE` |
| `text`, `varchar`, `char`, `uuid`, `enum`, `inet`, etc. | `STRING` | `NULLABLE` |
| Array types (e.g. `integer[]`, `text[]`) | Element Type | `REPEATED` |

---

## 🏗️ Project Architecture

```
PGTOBIGQUERY/
├── app/
│   ├── __init__.py
│   ├── config.py              # Environment configuration loader
│   ├── type_mapper.py         # Postgres 10.4 -> BigQuery schema converter
│   ├── extractor.py           # Postgres metadata & server-side cursor extractor
│   ├── loader.py              # PyArrow Parquet converter & BigQuery API uploader
│   ├── migrator.py           # Migration orchestrator & status tracker
│   ├── cli.py                 # Rich CLI command-line interface
│   ├── web.py                 # FastAPI backend server
│   └── templates/
│       └── index.html         # Glassmorphism Web Dashboard UI
├── tests/
│   ├── test_type_mapper.py    # Unit tests for schema mapping
│   └── test_extractor.py      # Unit tests for configuration & extraction
├── Dockerfile                 # Multi-stage container build
├── docker-compose.yml         # Container orchestration (App + Postgres 10.4)
├── init_pg10_sample.sql       # Sample database seeding script
├── requirements.txt           # Python package dependencies
├── .env.example               # Configuration template
└── README.md                  # Documentation
```

---

## ⚡ Quickstart Guide

### Option 1: Running with Docker Compose (Recommended)

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/edjiesa/PGTOBIGQUERY.git
   cd PGTOBIGQUERY
   ```

2. **Set up Google Cloud Credentials**:
   Place your Google Cloud Service Account JSON key inside the project directory as `gcp-service-account.json`.

3. **Configure Environment (`.env`)**:
   Copy `.env.example` to `.env` and adjust your BigQuery project & dataset settings:
   ```bash
   cp .env.example .env
   ```
   *Edit `.env`*:
   ```env
   GCP_PROJECT_ID=your-gcp-project-id
   BIGQUERY_DATASET_ID=pg10_migrated_db
   ```

4. **Start Container Stack**:
   ```bash
   docker-compose up --build
   ```
   - PostgreSQL 10.4 test database will automatically initialize with sample tables (`users`, `orders`, `products`).
   - Access the **Web Dashboard** at `http://localhost:8000`.

---

### Option 2: Running via Command Line (CLI)

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Test Database Connections**:
   ```bash
   python -m app.cli test-connection
   ```

3. **Preview Schema Mapping**:
   ```bash
   python -m app.cli schema-preview --schema public
   ```

4. **Run Migration (Dry Run)**:
   ```bash
   python -m app.cli migrate --dry-run
   ```

5. **Execute Full Migration**:
   ```bash
   python -m app.cli migrate --batch-size 50000
   ```

6. **Launch Web UI**:
   ```bash
   python -m app.cli serve --host 0.0.0.0 --port 8000
   ```

---

## 🧪 Running Unit Tests

Run unit tests using `pytest`:

```bash
pytest -v
```

---

## 📜 License

Distributed under the MIT License. See `LICENSE` for details.
