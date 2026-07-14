# Data Engineering Project with Databricks Lakehouse

![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-red)
![PySpark](https://img.shields.io/badge/PySpark-3.x-orange)
![Unity Catalog](https://img.shields.io/badge/Unity%20Catalog-enabled-blue)
![DLT](https://img.shields.io/badge/Lakeflow-Declarative%20Pipelines-green)
![License](https://img.shields.io/badge/license-MIT-lightgrey)

A complete **Data Engineering** project built on **Databricks** following the **Medallion Architecture (Bronze → Silver → Gold)**.

The project demonstrates how to ingest raw data, perform cleansing and transformations, enforce data quality, build dimensional models, and orchestrate the entire workflow using **Databricks Asset Bundles** and **Spark Declarative Pipelines**.

---

# Architecture

```text
                    Raw CSV Files
                          │
                          ▼
                 Landing Volume (Unity Catalog)
                          │
                          ▼
                Bronze Layer (Raw Tables)
                          │
                          ▼
             Silver Layer (Cleaned & Standardized)
                          │
                          ▼
        Gold Layer (Star Schema & Analytics)
                          │
                          ▼
             Dashboards / Business Intelligence
```

---

# Technologies

- Databricks
- PySpark
- Spark Declarative Pipelines (Lakeflow)
- Databricks Asset Bundles
- Unity Catalog
- Delta Lake
- Materialized Views
- Serverless Pipelines
- YAML
- Python

---

# Repository Structure

```
dmc_final/
│
├── dashboard/
│   └── dashboard_dmc_final.lvdash.json
│
├── resources/
│   ├── job_dmc_final.job.yml
│   └── pipeline_dmc_final.yml
│
├── src/
│   ├── init/
│   │     └── setup.ipynb
│   │
│   └── pipeline_dmc_final/
│         └── transformations/
│               ├── 01_bronze.py
│               ├── 02_silver.py
│               └── 03_gold.py
│
├── tests/
│
├── databricks.yml
└── pyproject.toml
```

---

# Project Workflow

## 1. Environment Setup

The initialization notebook prepares the project environment and configures the required Unity Catalog objects before executing the pipeline.

---

## 2. Bronze Layer

The Bronze layer ingests raw source files from the Landing Volume into Delta tables.

Responsibilities include:

- Raw data ingestion
- Schema definition

---

## 3. Silver Layer

The Silver layer performs data cleansing and standardization.

Typical operations include:

- Type conversions
- Null handling

---

## 4. Gold Layer

The Gold layer creates an analytical model optimized for reporting.

Current objects include:

### Dimensions

- Customer Dimension (`dim_cliente`)
- Product Dimension (`dim_producto`)
- Date Dimension (`dim_fecha`)

### Fact Table

- Sales Fact (`fact_ventas`)

These tables are implemented as **Materialized Views**, allowing efficient analytical queries.

---

# Data Quality

The project uses **Spark Declarative Pipeline Expectations** to guarantee data quality.

Examples include:

- Positive product prices
- Non-negative stock
- Positive sales quantity
- Valid discounts
- Valid customer references
- Valid product references

Example:

```python
@dp.expect_or_drop(
    "precio_unitario_positive",
    "precio_unitario > 0"
)
```

Rows violating these rules are automatically discarded.

---

# Configuration

The project uses a parameterized `databricks.yml` allowing deployment to multiple environments.

Current targets include:

- Development
- Production

Variables include:

- Catalog
- Landing Schema
- Bronze Schema
- Silver Schema
- Gold Schema
- Landing Volume

---

# Databricks Asset Bundles

Infrastructure is defined as code.

Resources include:

- Declarative Pipeline
- Databricks Job
- Dashboard
- Variables
- Deployment Targets

This enables reproducible deployments across environments.

---

# Pipeline Orchestration

The Databricks Job executes:

```
Setup Notebook
        │
        ▼
Declarative Pipeline
        │
        ▼
Bronze
        │
        ▼
Silver
        │
        ▼
Gold
```

---

# Dashboard

The repository also contains a Databricks Dashboard used for business reporting and KPI visualization.

```
dashboard/
    dashboard_dmc_final.lvdash.json
```

---

# Deployment

Deploy the bundle

```bash
databricks bundle deploy
```

Run the job

```bash
databricks bundle run job_dmc_final
```

Validate configuration

```bash
databricks bundle validate
```

---

# Features

- Medallion Architecture
- Unity Catalog integration
- Serverless Pipelines
- Data Quality Expectations
- Materialized Views
- Declarative ETL
- Parameterized deployments
- Dashboard integration

---

# Author

**Miquel Molinier**

Data Engineer | PySpark | Databricks | SQL | Delta Lake | Unity Catalog

GitHub:
https://github.com/MiquelMolinier