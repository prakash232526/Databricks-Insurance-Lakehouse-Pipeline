# Databricks Insurance Lakehouse Pipeline

An end-to-end **Databricks Lakehouse** project that processes **insurance policy and claims data** using a **Medallion architecture (Bronze / Silver / Gold)** with **Delta Lake**, **MERGE-based upserts**, **data quality checks**, and **watermark-driven incremental processing**.

## Project Overview

This project simulates a realistic insurance data engineering workflow in Databricks. Raw CSV files for **policy** and **claims** data are loaded into Bronze Delta tables from a managed volume. The data is then standardized, deduplicated, and upserted into curated Silver tables. Finally, Gold tables provide business-ready analytics, and operational notebooks add quality checks and incremental control-table processing.

The goal of this project was not just to build a basic ETL flow, but to model the kinds of patterns used in production data engineering systems:

- Medallion architecture
- Delta Lake tables
- MERGE / upsert logic
- Watermark-based incremental processing
- Lookback window for late-arriving data
- Data quality validations
- Pipeline control / run metadata tracking

---

## Architecture

<img width="1800" height="1100" alt="databricks_insurance_architecture" src="https://github.com/user-attachments/assets/66e63a44-4667-40a6-934c-01e8108ed0d1" />


### High-Level Flow

**Managed Volume CSV Files**  
→ **Bronze Delta Tables**  
→ **Silver Curated Delta Tables**  
→ **Gold Analytics Tables**  
→ **Quality Checks + Incremental Control Table**

---

## Tech Stack

- **Databricks Free Edition**
- **PySpark**
- **Delta Lake**
- **Unity Catalog / Managed Volumes**
- **SQL + DataFrame API**
- **Delta MERGE**
- **Control Table for Incremental Processing**

---

## Project Structure

### Notebooks

- `01_setup`
- `02_bronze_policy_claims`
- `03_silver_policy_claims`
- `04_gold_analytics`
- `05_quality_checks`
- `06_incremental_silver_control`

### Schema

- `workspace.insurance_lakehouse`

### Managed Volume Path

- `/Volumes/workspace/default/insurance_data/`

### Input Files

- `policy_data_databricks.csv`
- `claims_data_databricks.csv`

---

## Notebook-by-Notebook Explanation

### 1. `01_setup`
Initializes the project environment and schema.

**What it does**
- Creates / uses the target schema
- Sets catalog and schema context
- Prepares the namespace used across the project

**Purpose**
Keeps the project organized and reusable across notebooks.

---

### 2. `02_bronze_policy_claims`
Loads raw CSV data from the managed volume into Bronze Delta tables.

**What it does**
- Reads policy and claims CSV files
- Normalizes column names
- Adds ingestion metadata:
  - `source_file`
  - `ingestion_ts`
  - `snapshot_date`
- Writes Bronze Delta tables:
  - `bronze_policy`
  - `bronze_claims`

**Purpose**
Bronze acts as the **raw landing layer**, preserving source data with lineage metadata.

---

### 3. `03_silver_policy_claims`
Transforms Bronze data into curated Silver tables.

**What it does**
- Reads Bronze tables
- Casts date and timestamp fields
- Deduplicates by business key:
  - `policy_id`
  - `claim_id`
- Selects the latest record per key using `updated_ts`
- Uses **Delta MERGE** to upsert into:
  - `silver_policy`
  - `silver_claims`

**Purpose**
Silver is the **curated and trusted layer** used for downstream analytics.

---

### 4. `04_gold_analytics`
Builds business-facing analytics tables from Silver.

**What it does**
- Aggregates claim data by:
  - `claim_month`
  - `claim_type`
  - `claim_status`
- Calculates:
  - `claim_count`
  - `total_claim_amount`

**Gold Table**
- `gold_claim_summary`

**Purpose**
Gold is the **analytics-ready layer** designed for reporting and BI consumption.

---

### 5. `05_quality_checks`
Validates pipeline outputs.

**What it does**
- Bronze row-count checks
- Silver row-count checks
- Duplicate key checks
- Null key checks
- Claim-to-policy referential integrity check

**Quality Table**
- `quality_report`

**Purpose**
Demonstrates production-style pipeline validation and data quality awareness.

---

### 6. `06_incremental_silver_control`
Adds incremental and operational logic for Bronze-to-Silver processing.

**What it does**
- Reads the last successful watermark from a control table
- Applies a **lookback window**
- Filters recent Bronze records based on `event_ts`
- MERGEs incremental rows into Silver
- Writes run metadata to:
  - `pipeline_control`

**Control Columns**
- `pipeline_name`
- `source_table`
- `target_table`
- `last_watermark_ts`
- `lookback_days`
- `rows_processed`
- `run_status`
- `run_ts`

**Purpose**
Makes the Silver layer more production-like by supporting:
- incremental processing
- rerun safety
- late-arriving data handling
- operational observability

---

## Data Flow

### Bronze
Raw landing layer with ingestion metadata.

### Silver
Curated layer with:
- schema standardization
- latest-record logic
- MERGE-based upsert behavior

### Gold
Business-ready aggregated analytics layer.

---

## Incremental Processing Design

This project includes **watermark-based incremental processing** with a **lookback window**.

### Why watermark?
The watermark stores the latest successfully processed event timestamp.

### Why lookback?
A strict watermark can miss delayed data. The lookback window intentionally reprocesses a recent slice of data so late-arriving records are not skipped.

### Why MERGE?
MERGE makes reruns safe:
- matching business keys are updated
- new business keys are inserted
- curated tables stay clean and deduplicated

---

## Late-Arriving Data Test

To validate the design, a newer version of an existing claim record was manually appended to the Bronze table with a later `updated_ts`. After rerunning the incremental claims notebook:

- the record was picked up by incremental filtering
- the latest version won in the deduplication logic
- the Silver table was updated through Delta MERGE
- the control table logged the new run

This demonstrated that the pipeline correctly handles late-arriving updates.

---

## Key Tables

### Bronze
- `workspace.insurance_lakehouse.bronze_policy`
- `workspace.insurance_lakehouse.bronze_claims`

### Silver
- `workspace.insurance_lakehouse.silver_policy`
- `workspace.insurance_lakehouse.silver_claims`

### Gold
- `workspace.insurance_lakehouse.gold_claim_summary`

### Quality / Control
- `workspace.insurance_lakehouse.quality_report`
- `workspace.insurance_lakehouse.pipeline_control`

---

## Example Quality Checks

- Bronze tables must have rows
- Silver tables must have rows
- `policy_id` should be unique in Silver
- `claim_id` should be unique in Silver
- Key columns should not be null
- Every claim must reference a valid policy

---

## Why This Project Matters

This project demonstrates practical data engineering patterns that are directly relevant in real-world environments:

- building Medallion pipelines in Databricks
- designing curated Delta tables
- handling updates with MERGE
- implementing incremental processing
- protecting against late-arriving data
- adding pipeline quality checks
- tracking operational metadata in control tables

---

## How to Run

1. Upload the source CSV files into the managed volume:
   - `/Volumes/workspace/default/insurance_data/`
2. Run notebooks in order:
   1. `01_setup`
   2. `02_bronze_policy_claims`
   3. `03_silver_policy_claims`
   4. `04_gold_analytics`
   5. `05_quality_checks`
   6. `06_incremental_silver_control`
3. To test late-arriving data:
   - append a newer version of an existing Bronze record
   - rerun the relevant incremental notebook
   - verify Silver updates and control-table logs

---

## Screenshots

Recommended screenshots to include in the repository:

1. **Project notebook list**
  <img width="584" height="191" alt="image" src="https://github.com/user-attachments/assets/d14d11d2-80db-4940-9f68-0399b114be99" />


2. **Bronze layer sample**
 <img width="975" height="374" alt="image" src="https://github.com/user-attachments/assets/cd786c1a-62da-4590-9548-37ee3400ce63" />


3. **Silver layer sample**
<img width="975" height="321" alt="image" src="https://github.com/user-attachments/assets/6ecd1860-49ba-42df-89c8-698d9b6b3651" />

4. **Gold analytics output**
<img width="975" height="811" alt="image" src="https://github.com/user-attachments/assets/13ea941d-3277-4201-9a25-83dbc7f86e1e" />

5. **Quality report**
<img width="975" height="731" alt="image" src="https://github.com/user-attachments/assets/bef6c297-2fd7-48e5-be69-0b31cb41a2eb" />


6. **Pipeline control table**
<img width="975" height="220" alt="image" src="https://github.com/user-attachments/assets/1efe0542-dcc4-450f-9e27-072c4a318ed4" />


---

