# 🚀 PostgreSQL 10.4 to Google BigQuery Migration Tool (Dockerized)

![PostgreSQL to BigQuery](https://img.shields.io/badge/PostgreSQL-10.4%2B_Remote-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![Google BigQuery](https://img.shields.io/badge/Google_BigQuery-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white)
![Airbyte Diagnostic](https://img.shields.io/badge/Airbyte--Style-Tester-647eee?style=for-the-badge)
![Docker Standalone](https://img.shields.io/badge/Docker-Standalone_App-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Python 3.11+](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)

A high-performance, containerized database migration tool designed to migrate schema and data directly from **any remote PostgreSQL 10.4+ database** (AWS RDS, GCP Cloud SQL, Azure PostgreSQL, on-premise remote servers, etc.) to **Google BigQuery**. 

No local PostgreSQL installation inside Docker is required! Runs as a standalone app container.

---

## 🌟 Key Features

1. **Connect to Multiple Remote PostgreSQL Databases**:
   - Save and switch between multiple **Remote Connection Profiles** (e.g. `AWS RDS Production`, `GCP Cloud SQL Staging`, `On-Premise Server`).
   - Supports custom remote Host IP / Domain Names, Ports, Database Names, and SSL Modes (`disable`, `require`, `prefer`, `verify-ca`, `verify-full`).
2. **Airbyte-Style Live Connection Diagnostics**:
   - Step-by-step diagnostic checklist for both **Remote PostgreSQL** and **Google BigQuery**.
   - Validates TCP Handshake, SSL Connection, Database Authentication, Catalog Permissions, Service Account JSON key format, OAuth2 authentication, and Target Dataset access.
3. **GUI Service Account JSON Key Input & Uploader**:
   - Paste GCP Service Account JSON key directly into the Web UI text area or upload via the `📁 Upload .json` button.
4. **Standalone Docker Container**:
   - Runs as a lightweight single container app (`docker-compose up --build`).
   - Connects directly to external PostgreSQL IPs or host machine (`host.docker.internal`).
5. **High-Speed Parquet Data Pipeline**:
   - Uses PostgreSQL server-side cursors (`FETCH N`) to stream data with minimal memory footprint.
   - Converts batches into Apache Parquet files (`pyarrow`) and ingests via BigQuery Parquet Load API (`load_table_from_file`).

---

## ⚡ Quickstart Guide (Standalone Docker)

### Step 1: Clone the Repository
```bash
git clone https://github.com/edjiesa/PGTOBIGQUERY.git
cd PGTOBIGQUERY
```

### Step 2: Start Standalone Container App
```bash
docker-compose up --build
```

### Step 3: Open Web Dashboard & Manage Remote Profiles
Open your browser at `http://localhost:8000`:
1. Enter your **Remote PostgreSQL Host / IP** (e.g., `10.0.1.50`, `db.company.com`, or `host.docker.internal`).
2. Select your **SSL Mode** (`prefer`, `require`, etc.).
3. Save the connection profile by clicking **`💾 Save Profile`**.
4. Paste your **GCP Service Account JSON Key** or upload via `📁 Upload .json`.
5. Click **`🔍 Airbyte Test Remote PostgreSQL`** and **`🔍 Airbyte Test BigQuery`**.
6. Click **`⚡ Jalankan Migrasi Remote`** to migrate your remote database tables!

---

## 🛠️ Optional: Running Development Postgres in Docker

If you want to test locally with a PostgreSQL 10.4 test container:

```bash
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up --build
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
