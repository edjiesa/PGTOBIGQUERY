# 🚀 PostgreSQL 10.4 to Google BigQuery Migration Tool (Dockerized)

![PostgreSQL to BigQuery](https://img.shields.io/badge/PostgreSQL-10.4-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![Google BigQuery](https://img.shields.io/badge/Google_BigQuery-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white)
![Airbyte Diagnostic](https://img.shields.io/badge/Airbyte--Style-Tester-647eee?style=for-the-badge)
![Docker Compose](https://img.shields.io/badge/Docker_Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Python 3.11+](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)

A high-performance, containerized database migration tool designed specifically to migrate schema and data from **PostgreSQL 10.4** to **Google BigQuery**. 

Features an **Apache Parquet streaming engine**, an **Airbyte-Style Live Connection Diagnostic Tester**, a **Service Account JSON Key Editor**, and a **Modern Web Dashboard (Web UI)** with real-time migration logs over WebSockets.

---

## 🌟 Key Features

1. **Airbyte-Style Live Connection Diagnostics**:
   - Step-by-step diagnostic checklist for both **PostgreSQL** and **Google BigQuery**.
   - Validates Service Account JSON key format, RSA credentials parsing, OAuth2 authentication, GCP Project access, BigQuery API permissions, and target dataset access.
2. **GUI Service Account JSON Key Input & Uploader**:
   - Paste GCP Service Account JSON key directly into the Web UI text area or upload via the `📁 Upload .json` button.
   - Credentials are held securely in session memory for seamless browser-based setup.
3. **PostgreSQL 10.4 Type Compatibility**:
   - Explicitly inspects PostgreSQL 10 catalog metadata (`information_schema` & `pg_catalog`).
   - Accurately converts Postgres 10 data types (`integer`, `bigint`, `numeric`, `double precision`, `boolean`, `json`, `jsonb`, `uuid`, `timestamp with/without timezone`, `date`, `bytea`, and `ARRAY` types like `text[]`).
4. **High-Speed Parquet Data Pipeline**:
   - Uses PostgreSQL server-side cursors (`FETCH N`) to stream data with minimal memory footprint.
   - Converts batches into in-memory/temporary Apache Parquet files (`pyarrow`).
   - Ingests data directly using Google BigQuery's high-speed Parquet Load API (`load_table_from_file`).
5. **Dual User Interface**:
   - **Modern Web Dashboard**: Glassmorphism UI built with FastAPI + HTML5/CSS3 with Airbyte diagnostic cards & live WebSocket progress logs.
   - **Rich Terminal CLI**: Interactive colored progress bars, tables, and status screens.
6. **Docker & Docker-Compose Ready**:
   - Includes `Dockerfile` and `docker-compose.yml` bundled with a `postgres:10.4-alpine` test container preloaded with sample test data.

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

## ⚡ Quickstart Guide (Web UI / Docker)

### Option 1: Running with Docker Compose (Recommended)

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/edjiesa/PGTOBIGQUERY.git
   cd PGTOBIGQUERY
   ```

2. **Start Container Stack**:
   ```bash
   docker-compose up --build
   ```
   - PostgreSQL 10.4 test database will automatically initialize with sample tables (`users`, `orders`, `products`).

3. **Open Web Dashboard**:
   Go to `http://localhost:8000` in your web browser.

4. **Input Credentials & Test Connection (Airbyte-Style)**:
   - Enter your **PostgreSQL Connection Details** (Host, Port, User, Password, Database, Schema).
   - Enter your **GCP Project ID** and **BigQuery Dataset ID**.
   - Paste your **GCP Service Account JSON Key** directly into the JSON Key textarea or click **`📁 Upload .json`**.
   - Click **`🔍 Airbyte Test BigQuery`** and **`🔍 Airbyte Test PostgreSQL`** to verify all diagnostic checks pass (`CONNECTED` / `AUTHENTICATED`).
   - Click **`⚡ Jalankan Migrasi`** to start migrating tables to Google BigQuery!

---

### Option 2: Running via Command Line (CLI)

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Set Configuration (`.env`)**:
   ```bash
   cp .env.example .env
   ```

3. **Test Database Connections**:
   ```bash
   python -m app.cli test-connection
   ```

4. **Preview Schema Mapping**:
   ```bash
   python -m app.cli schema-preview --schema public
   ```

5. **Run Migration (Dry Run)**:
   ```bash
   python -m app.cli migrate --dry-run
   ```

6. **Execute Full Migration**:
   ```bash
   python -m app.cli migrate --batch-size 50000
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
