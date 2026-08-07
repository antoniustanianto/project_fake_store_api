# End-to-End Retail Analytics ELT Pipeline

> Building a reliable retail analytics pipeline that transforms operational REST API data into trusted, analytics-ready datasets using modern Data Engineering practices.

---

# Overview

This project demonstrates how operational retail data exposed through REST APIs can be transformed into reliable datasets for business analytics.

Using a modern ELT architecture, the pipeline automatically ingests raw data from the Fake Store API, stores it in Google BigQuery, transforms it into analytics-ready models with dbt, validates data quality, and serves downstream business reporting through Power BI.

---

# Business Problem

Retail applications commonly expose operational data through REST APIs. While suitable for transactional systems, these API responses are not designed for analytical workloads.

Before business users can analyze sales performance, customer behavior, or product trends, the data must first be collected, standardized, validated, and transformed into a consistent analytical model.

Without a structured data pipeline, organizations often face challenges such as:

- Nested JSON responses that are difficult to analyze directly
- Raw operational data that is not analytics-ready
- Manual data preparation before reporting
- Inconsistent data structures across sources
- Lack of automated data quality validation
- Repetitive reporting processes

---

# Solution

This project implements an automated ELT pipeline that transforms operational retail data into trusted datasets ready for business analytics.

The pipeline automatically:

- Extracts operational data from the Fake Store API
- Loads raw data into Google BigQuery for traceability
- Transforms raw datasets into analytics-ready models using dbt
- Validates data quality before downstream consumption
- Delivers clean datasets for Power BI reporting

---

# Architecture

```text
                 Fake Store API
                        │
                        ▼
               Apache Airflow
                        │
                        ▼
              Google BigQuery (Raw)
                        │
                        ▼
                dbt Staging Models
                        │
                        ▼
                 dbt Mart Models
                        │
                        ▼
              Data Quality Validation
                        │
                        ▼
                 Power BI Dashboard
```

> *(Replace this section later with an architecture diagram.)*

---

# Engineering Decisions

## Why ELT?

The project follows an ELT architecture by loading raw API responses into BigQuery before applying transformations.

Keeping raw data enables transformation logic to evolve as business requirements change without re-ingesting data from the source system.

---

## Why BigQuery?

BigQuery provides a scalable cloud data warehouse capable of storing raw operational data while efficiently supporting analytical workloads.

---

## Why dbt?

dbt enables modular SQL transformations, separating staging and business models into reusable components that are easier to maintain and test.

---

## Why Great Expectations?

Automated validation helps ensure downstream datasets remain reliable before being consumed by reporting dashboards.

---

# Pipeline Stages

| Stage | Purpose |
|-------|---------|
| Extract | Retrieve operational retail data from the Fake Store API |
| Load | Store raw operational data in BigQuery |
| Transform | Build analytics-ready datasets using dbt |
| Validate | Execute automated data quality checks |
| Serve | Deliver trusted datasets for Power BI reporting |

---

# Data Warehouse Architecture

This project follows a three-layer warehouse architecture.

| Layer | Purpose |
|--------|---------|
| Raw | Stores original API responses for traceability |
| Staging | Cleans and standardizes source data |
| Mart | Business-ready datasets optimized for analytics |

---

# Data Quality

Automated validation is applied before downstream reporting to improve confidence in analytical datasets.

Examples include:

- Null value checks
- Unique key validation
- Accepted value validation
- Schema consistency
- Relationship validation

> *(Add Great Expectations screenshots later.)*

---

# Business Output

The transformed datasets support analytical reporting such as:

- Revenue by product category
- Product performance analysis
- Customer purchasing behavior
- Sales distribution insights

> *(Insert Power BI dashboard screenshots here.)*

---

# Engineering Highlights

- Built an automated end-to-end ELT pipeline using Apache Airflow
- Implemented a cloud data warehouse with Raw → Staging → Mart architecture
- Developed modular dbt transformation models following reusable design principles
- Applied automated data quality validation before downstream reporting
- Containerized the complete development environment with Docker Compose

---

# Tech Stack

| Component | Technology |
|------------|------------|
| Programming | Python |
| Orchestration | Apache Airflow 2.9.1 |
| Data Warehouse | Google BigQuery |
| Transformation | dbt Core |
| Data Quality | Great Expectations |
| Visualization | Power BI |
| Containerization | Docker Compose |

---

# Repository Structure

```text
.
├── dags/
├── dbt/
├── great_expectations/
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

# Local Setup

## 1. Clone repository

```bash
git clone <repository-url>
```

## 2. Add Google Cloud credentials

Place your Service Account key in:

```text
keys/gcp-service-account.json
```

## 3. Start services

```bash
docker compose up -d
```

## 4. Open Airflow

```
http://localhost:8080
```

Default credentials:

```
Username : admin
Password : admin
```

---

# Future Improvements

Potential enhancements for a production-ready implementation:

- Incremental loading
- CI/CD pipeline
- Automated monitoring & alerting
- Data lineage documentation
- Partitioning & clustering optimization
- Cost optimization for BigQuery workloads
- Unit testing for transformation logic
