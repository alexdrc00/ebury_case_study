# Customer Transactions Data Platform

> **End-to-end customer transactions data pipeline**
> 
> Ebury Case Study

---

## 📋 Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Key Features](#key-features)
- [Data Model](#data-model)
- [Monitoring](#monitoring)

---

## 🎯 Overview

This project implements a production-grade data pipeline that:
- Ingests customer transaction data from CSV
- Cleans and validates data quality issues
- Builds a dimensional data warehouse
- Provides comprehensive quality monitoring

---

## 🚀 Quick Start

### Prerequisites

- Docker Desktop or Docker + Docker Compose
- 4GB+ RAM available
- 5GB+ disk space

### Installation

```bash
# 1. Clone repository
git clone 
cd customer-transactions-platform

# 2. Create environment file
cp .env.example .env
# Edit .env if needed

# Generate aiflow key
python3 utils/generate_fernet_key.py

# 3. Start services
docker-compose up -d

# 4. Wait for services to be healthy (~1 minute)
docker-compose ps
"
```

### First Run

```bash
# Access Airflow UI
open http://localhost:8080
# Login: admin / admin

# Enable and trigger the DAG
# 1. Find: customer_transactions_pipeline
# 2. Toggle to ON
# 3. Monitor execution (~2 minutes)
```

### Verify Success

```bash
# Run verification queries -- See test_queries.sql

# Option 1: execute the queries with the psql client
docker-compose exec postgres psql -U airflow -d analytics

# Option 2: access pgadmin for a GUI
open localhost:5050
# Explore DB and Schemas and use the Query Tool on the Analytics DB
```

### View dbt Documentation

```bash
docker-compose exec airflow-webserver bash -c "
  cd /opt/airflow/dbt_project
  dbt docs generate
  dbt docs serve --host 0.0.0.0 --port 8081
"
open http://localhost:8081
```

---

## 🏗️ Architecture

### Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Orchestration | Apache Airflow 2.9.3 | Workflow management |
| Transformation | dbt 1.8.x | SQL-based transformations |
| Database | PostgreSQL 13 | Data warehouse |
| Containerization | Docker Compose | Environment management |
| Language | Python 3.x | Pipeline logic |

### Pipeline Layers

1. **Raw Layer** (`raw` schema)
   - Stores source data as-is
   - Minimal transformation
   - TEXT data types for flexibility

2. **Staging Layer** (`staging` schema)
   - Cleans data quality issues
   - Standardizes formats
   - Type conversions
   - Data quality flags

3. **Mart Layer** (`mart` schema)
   - Dimensional model (star schema)
   - Business-ready tables
   - Pre-aggregated analytics

---

## ✨ Key Features

### 🔧 Modules and Utilities

- **db_utils**
  - `DatabaseManager`: Consistent database operations
  - `DataQualityChecker`: Declarative quality validation

- **test_queries**
  - Test queries to validate result
  - Checks for completeness and integrity
  - Extract business aggregates results

- **macros**
  - `try_cast_date`: Handle possible MM-DD switch ups
  - `try_cast_numeric`: Hnadle wrong numerics

### 📊 Data Quality

**Handles Multiple Issues:**
Example issues addressed:
- ✅ Inconsistent date formats (DD-MM-YYYY → YYYY-MM-DD)
- ✅ Text in numeric fields ("Two Hundred" → 200.00)
- ✅ Missing values (NULL handling)
- ✅ Invalid ID prefixes (T1010 → 1010)
- ✅ Type conversions (TEXT → INTEGER/NUMERIC/DATE)

**Automated Checks:**
- Set up automated checks at all layers: raw -> staging -> mart

**Quality Scoring:**
- 0-7 scale per transaction

### 🎯 Dimensional Modeling

**Star Schema Implementation:**
- `dim_customers`: Customer master with metrics
- `dim_products`: Product catalog with analytics
- `dim_dates`: Time dimension
- `fact_transactions`: Transaction-level facts

**Business Features:**
- Customer segmentation (value, tier, status)
- Product performance metrics
- Time-based analysis
- Pre-aggregated summaries

---

## 📐 Data Model

### Dimensional Model (Star Schema)

```
          dim_dates
              |
              |
dim_customers─┼─fact_transactions─dim_products
              |
          (grain: one row per transaction)
```

### Table Descriptions

| Table | Type | Grain |
|-------|------|-------|
| `dim_customers` | Dimension | One per customer |
| `dim_products` | Dimension | One per product |
| `dim_dates` | Dimension | One per date |
| `fact_transactions` | Fact | One per transaction |
| `monthly_customer_summary` | Aggregate | Customer-month |
| `product_performance` | Aggregate | Product-day |

---

## 📈 Monitoring

### Airflow UI

- DAG runs: http://localhost:8080
- Task logs: DAG -> Task -> Logs
- XCom data: DAG -> Task -> XCom

### PGAdmin

- Queries and Data model: localhost:5050

### Quality Reports

Stored in XCom after each run:
- `comprehensive_quality_report`: Full report
- `staging_quality_results`: Staging metrics
- `mart_quality_results`: Mart metrics

### Additional Logs

```bash
# View Airflow logs
docker-compose logs -f airflow-scheduler

# View dbt logs
ls -la dbt_project/logs/

# View PostgreSQL logs
docker-compose logs postgres
```

---

## 🙏 Acknowledgments

- Ebury for providing the chance to take on this case study

---
