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
- [Quality Framework](#quality-framework)
- [Project Structure](#project-structure)
- [Usage Examples](#usage-examples)
- [Testing](#testing)
- [Monitoring](#monitoring)
- [Technical Decisions](#technical-decisions)
- [Future Enhancements](#future-enhancements)

---

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

### Data Flow

```
```

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

### 🔧 Platform Engineering

- **Reusable Utilities**
  - `DatabaseManager`: Consistent database operations
  - `DataQualityChecker`: Declarative quality validation
  - Used across all DAG tasks

- **Declarative Patterns**
  - Quality checks defined as configuration
  - Easy to extend and maintain
  - Framework thinking over one-off scripts

- **Production Ready**
  - Comprehensive error handling
  - Detailed logging
  - Idempotent operations
  - Retry logic

### 📊 Data Quality

**Handles Multiple Issues:**
- ✅ Inconsistent date formats (DD-MM-YYYY → YYYY-MM-DD)
- ✅ Text in numeric fields ("Two Hundred" → 200.00)
- ✅ Missing values (NULL handling)
- ✅ Invalid ID prefixes (T1010 → 1010)
- ✅ Type conversions (TEXT → INTEGER/NUMERIC/DATE)

**26+ Automated Checks:**
- Raw layer: Completeness checks
- Staging layer: 10 validation rules
- Mart layer: 15 integrity checks

**Quality Scoring:**
- 0-5 scale per transaction
- Tracks specific issues
- Comprehensive reporting

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

| Table | Type | Grain | Row Count |
|-------|------|-------|-----------|
| `dim_customers` | Dimension | One per customer | ~11 |
| `dim_products` | Dimension | One per product | 5 |
| `dim_dates` | Dimension | One per date | ~25 |
| `fact_transactions` | Fact | One per transaction | ~92 |
| `monthly_customer_summary` | Aggregate | Customer-month | ~20 |
| `product_performance` | Aggregate | Product-day | ~125 |

### Sample Queries for Business Insights

```sql
-- Top customers by revenue
SELECT customer_name, total_revenue, customer_tier
FROM mart.dim_customers
WHERE NOT is_unknown_customer
ORDER BY total_revenue DESC
LIMIT 5;

-- Product performance
SELECT product_name, total_revenue, performance_segment
FROM mart.dim_products
ORDER BY total_revenue DESC;

-- Monthly revenue trend
SELECT year_month, SUM(total_revenue) as revenue
FROM mart.monthly_customer_summary
GROUP BY year_month
ORDER BY year_month;
```

---

## 🔍 Quality Framework

### Three-Layer Validation

**Layer 1: Raw Data**
- Row count validation
- File integrity checks

**Layer 2: Staging Data (10 checks)**
- NOT NULL validations (transaction_id, product_id, etc.)
- UNIQUE constraints (transaction_id)
- RANGE checks (price: 0-10000, quantity: 0-100)
- Calculated field integrity

**Layer 3: Mart Data (15 checks)**
- Foreign key integrity
- Primary key uniqueness
- Business rule validation
- Referential integrity

### Quality Metrics

From recent run:
```
Total Checks Run: 26
Passed: 24 ✓
Failed: 2 ✗
Pass Rate: 92.3%
```

**Known Issues (Expected):**
- NULL customer_id values (8 transactions)
- Invalid quantity values (3 transactions)

These are tracked in `data_quality_score` and flagged for reporting.

---

## 🧪 Testing

### Test Coverage

- **dbt Tests:** 130+ automated tests
- **Quality Checks:** 26+ validations
- **Coverage:** 100% of critical columns

### Test Types

- `not_null`: Column completeness
- `unique`: Primary key integrity
- `relationships`: Foreign key validity
- `accepted_values`: Categorical constraints
- `range`: Numeric bounds
- Custom business rules

---

## 📈 Monitoring

### Airflow UI

- DAG runs: http://localhost:8080
- Task logs: DAG -> Task -> Logs
- XCom data: DAG -> Task -> XCom

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

## 🎨 Technical Decisions

### Why DatabaseManager?

**Problem:** Repetitive database code in every task
**Solution:** Reusable utility with consistent patterns

**Benefits:**
- DRY code (Don't Repeat Yourself)
- Easier testing and mocking
- Consistent error handling
- Platform thinking

### Why DataQualityChecker?

**Problem:** Manual SQL for each quality check
**Solution:** Declarative check framework

**Benefits:**
- Easy to add new checks
- Consistent validation patterns
- Automatic aggregation
- Reusable across DAGs

### Why Star Schema?

**Problem:** Need fast analytics queries
**Solution:** Dimensional modeling with denormalization

**Benefits:**
- Simple join patterns
- Query performance
- Business-friendly structure
- Standard pattern

### Why dbt?

**Problem:** Need SQL transformations with testing
**Solution:** Modern data transformation tool

**Benefits:**
- SQL-based (accessible to analysts)
- Built-in testing framework
- Documentation generation
- Version control friendly

---

## 🔮 Future Enhancements

### Short Term

- [ ] Add incremental models for scale
- [ ] Implement SCD Type 2 for dimensions
- [ ] Add data freshness monitoring
- [ ] Create Slack alerts for failures

### Medium Term

- [ ] Add more aggregate tables
- [ ] Implement data lineage tracking
- [ ] Add performance benchmarks
- [ ] Create dbt macros for common patterns

### Long Term

- [ ] Add machine learning features
- [ ] Implement data catalog
- [ ] Add cost tracking
- [ ] Create self-service analytics layer

---

## 📊 Performance

**Pipeline Metrics:**
- Total runtime: ~5 minutes
- Raw load: ~5 seconds (100 rows)
- dbt staging: ~10 seconds
- dbt mart: ~20 seconds
- Quality checks: ~30 seconds

**Optimization Opportunities:**
- Incremental models (for larger datasets)
- Parallel task execution
- Connection pooling
- Index optimization

---

## 🙏 Acknowledgments

- Ebury for providing the chance to take on this case study

---
