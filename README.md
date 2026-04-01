# 🛒 Fake Store API — End-to-End Data Pipeline

An end-to-end ELT data pipeline that extracts data from the [Fake Store API](https://fakestoreapi.com), loads it into Google BigQuery, and transforms it using dbt — all orchestrated with Apache Airflow running on Docker.

---

## 🏗️ Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Fake Store API │────▶│  Apache Airflow  │────▶│ Google BigQuery │
│  (Data Source)  │     │  (Orchestration) │     │ (Data Warehouse)│
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                          │
                                                          ▼
                                                 ┌─────────────────┐
                                                 │       dbt       │
                                                 │ (Transformation)│
                                                 └─────────────────┘
```

### Pipeline Flow
```
01_extract_fakestore (Airflow DAG)
        │  Extract: products, users, carts → raw JSON
        ▼
02_load_bigquery (Airflow DAG)
        │  Load: raw JSON → BigQuery fakestore_raw
        ▼
fakestore_dbt_pipeline (Airflow DAG)
        │  Transform: raw → staging → marts
        ▼
   dbt run → dbt test (12 data quality checks)
```

---

## 🛠️ Tech Stack

| Tool | Version | Purpose |
|------|---------|---------|
| Apache Airflow | 2.9.1 | Pipeline orchestration |
| Google BigQuery | - | Cloud data warehouse |
| dbt (data build tool) | 1.7.0 | Data transformation & testing |
| Docker | - | Containerization |
| Python | 3.12 | DAG scripting |

### Prerequisites
- Docker Desktop (with WSL2 backend)
- Google Cloud account with BigQuery enabled
- GCP Service Account with BigQuery Admin role
- Python 3.8+

---

## 📁 Project Structure

```
project_fake_store_api/
├── dags/
│   ├── 01_extract_fakestore.py     # Extract data from Fake Store API
│   ├── 02_load_bigquery.py         # Load raw JSON to BigQuery
│   └── dag_fakestore_dbt.py        # Run dbt models & tests
├── dbt/
│   ├── profiles.yml                # dbt connection config
│   └── project_fake_store_api/
│       ├── dbt_project.yml
│       └── models/
│           ├── staging/
│           │   ├── sources.yml
│           │   ├── schema.yml
│           │   ├── stg_users.sql
│           │   ├── stg_products.sql
│           │   └── stg_carts.sql
│           └── marts/
│               ├── mart_cart_details.sql
│               ├── mart_user_summary.sql
│               ├── mart_product_summary.sql
│               └── mart_category_summary.sql
├── keys/                           # GCP Service Account (gitignored)
├── plugins/
├── tests/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .gitignore
```

---

## 🔄 dbt Models

### Staging Layer (`fakestore_staging`)
Clean and rename raw data from BigQuery.

| Model | Type | Description |
|-------|------|-------------|
| `stg_users` | View | Cleaned users with flattened name & address |
| `stg_products` | View | Cleaned products with flattened rating |
| `stg_carts` | View | Cleaned carts with unnested products array |

### Marts Layer (`fakestore_marts`)
Business-level aggregations for analytics.

| Model | Type | Description |
|-------|------|-------------|
| `mart_cart_details` | Table | Cart line items joined with users & products |
| `mart_user_summary` | Table | Total spending & purchase history per user |
| `mart_product_summary` | Table | Sales performance per product |
| `mart_category_summary` | Table | Sales aggregated by product category |

### Data Quality Tests
12 automated tests covering:
- `not_null` — critical columns have no null values
- `unique` — primary keys are unique
- `relationships` — referential integrity between models

---

## 🚀 How to Run

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/project_fake_store_api.git
cd project_fake_store_api
```

### 2. Setup GCP Service Account
- Create a Service Account in GCP with BigQuery Admin role
- Download the JSON key file
- Place it in `keys/` folder

### 3. Configure dbt profile
Edit `dbt/profiles.yml`:
```yaml
project_fake_store_api:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: YOUR_GCP_PROJECT_ID
      dataset: fakestore
      location: asia-southeast2
      keyfile: /opt/airflow/keys/YOUR_KEY_FILE.json
      threads: 4
      timeout_seconds: 300
```

### 4. Setup Airflow BigQuery connection
- Open Airflow UI → Admin → Connections
- Add connection `google_cloud_default` with your GCP credentials

### 5. Start the pipeline
```bash
# Build and start containers
docker compose up -d --build

# Access Airflow UI
open http://localhost:8080
# Default credentials: airflow / airflow
```

### 6. Trigger the pipeline
- In Airflow UI, trigger `01_extract_fakestore` manually
- It will automatically chain to `02_load_bigquery` → `fakestore_dbt_pipeline`

---

## 📊 BigQuery Dataset Structure

```
de-crypto-project
├── fakestore_raw        # Raw data from API
│   ├── users
│   ├── products
│   └── carts
├── fakestore_staging    # Cleaned & renamed (dbt views)
│   ├── stg_users
│   ├── stg_products
│   └── stg_carts
└── fakestore_marts      # Business layer (dbt tables)
    ├── mart_cart_details
    ├── mart_user_summary
    ├── mart_product_summary
    └── mart_category_summary
```

---

## 📝 License
MIT
