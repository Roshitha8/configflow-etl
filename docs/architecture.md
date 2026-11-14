# ConfigFlow ETL – Architecture & Technical Blueprint

This document details the 3-tier architectural design, metadata-driven ingestion mechanics, transaction lifecycle, security boundary model, and failure recovery policies of the ConfigFlow ETL platform.

---

## 1. System Architecture

The ConfigFlow ETL platform is strictly decoupled into three main application layers:

```mermaid
flowchart TD
    subgraph Presentation["Layer 1: Presentation (Streamlit)"]
        UI[frontend.py\nStreamlit Dashboard]
        UI -->|Invokes Pipelines| BE_API[Backend Service APIs]
        UI -->|Queries Metrics & Previews| DB_API[Database Query APIs]
    end

    subgraph Engine["Layer 2: Ingestion & Validation Engine"]
        BE[backend.py\nGeneric Ingestion Engine]
        CFG[(JSON Metadata Configs)] -->|Declares Schema, Modes & Rules| BE
        SRC[(Raw Sources: CSV / JSON)] -->|Stream / Batch Read| BE
        BE -->|Schema & Type Validation| VAL[Data Quality Validator]
        VAL -->|In-Batch Deduplication| DEDUP[Deduplication Engine]
        DEDUP -->|Decimal Math| TRANS[Transformation Engine]
        TRANS -->|FK Pre-Check| FK_CHK[Foreign Key Validator]
    end

    subgraph Storage["Layer 3: Relational Persistence & Audit"]
        DB[database.py\nSQLite Storage Layer]
        FK_CHK -->|Transactional Persistence| DB
        BE -->|Durable Run Audits| DB
        BE -->|Quarantined Records| DB
        DB --> WAREHOUSE[(SQLite Warehouse:\ncustomers, products, orders)]
        DB --> AUDIT[(Audit Ledger:\npipeline_runs, rejected_records,\norder_change_history)]
    end
```

---

## 2. Ingestion & Execution Lifecycle

Every ingestion execution follows a deterministic, 8-stage pipeline:

```mermaid
sequenceDiagram
    autonumber
    participant Caller as Caller (CLI / Streamlit)
    participant Engine as backend.py
    participant DB as database.py
    participant SQLite as SQLite Warehouse

    Caller->>Engine: run_pipeline(config_path, db_path, overrides)
    Engine->>Engine: Validate JSON Config & Allowlist Identifiers
    Engine->>DB: create_pipeline_run(run_id, status='RUNNING')
    DB->>SQLite: INSERT INTO pipeline_runs
    Engine->>Engine: Safely read source CSV/JSON (detect malformed lines)
    Engine->>Engine: Row-level type parsing & domain validations
    Engine->>Engine: In-batch primary key deduplication (first-wins)
    Engine->>Engine: Execute transformations (total_amount via Decimal)
    Engine->>DB: check_foreign_keys_bulk(parent_tables)
    DB->>SQLite: Query parent keys in chunks
    SQLite-->>Engine: Set of valid parent keys
    Engine->>DB: persist_records(table, mode, clean_records, run_id)
    Note over DB,SQLite: BEGIN TRANSACTION
    alt Load Mode == 'append'
        DB->>SQLite: Reject target duplicates, INSERT fresh rows
    else Load Mode == 'upsert'
        DB->>SQLite: INSERT new, UPDATE modified, SKIP identical, log history
    else Load Mode == 'replace'
        DB->>SQLite: Populate temp staging table, atomic swap
    end
    DB->>SQLite: INSERT INTO rejected_records (batch quarantine)
    Note over DB,SQLite: COMMIT TRANSACTION
    Engine->>DB: update_pipeline_run(status, metrics, elapsed_seconds)
    DB->>SQLite: UPDATE pipeline_runs
    Engine-->>Caller: Execution summary report
```

---

## 3. Core Architectural Mechanisms

### 3.1 Metadata-Driven Generic Loader
The ingestion engine contains **zero hardcoded branches** on dataset names. Whether processing `customers`, `products`, or `orders`, the engine dynamically consumes:
- Source specification: file format (`csv` / `json`), relative paths, options.
- Schema definition: column types (`string`, `integer`, `decimal`, `boolean`, `timestamp`), nullability.
- Primary and foreign key configurations: source column, parent table, parent column.
- Validation rules: minimum values, regex pattern matching, required non-nullable fields.
- Transformations: metadata-driven mathematical expressions using precise `Decimal` calculation.

### 3.2 SQL Identifier Validation & Parameterization
To prevent SQL injection vulnerabilities:
1. **Identifier Allowlist & Regex:** Every table name is validated against an allowed set (`ALLOWED_TABLES`) and checked with regex `^[a-zA-Z_][a-zA-Z0-9_]*$`.
2. **Quoted Identifiers:** All validated table and column names are quoted (e.g. `"{column}"`).
3. **Strict Parameterization:** No values are ever concatenated into SQL strings. All queries utilize parameterized statements (`?`).

### 3.3 Transaction Management & Rollback
- Operations within a load are wrapped inside a context manager (`database.transaction`).
- On any operational failure or database constraint violation:
  1. The active transaction is immediately rolled back (`conn.rollback()`).
  2. No partial updates are left in target tables.
  3. A separate, durable transaction records the run status as `FAILED` in `pipeline_runs` along with the failure reason and elapsed time.

### 3.4 Atomic Replace Semantics
In `replace` mode, rather than truncating the target table directly (which risks leaving an empty table if incoming data is bad):
1. A temporary staging table (`"{table}_staging_{run_id}"`) is created with the identical schema.
2. Clean accepted records are inserted into the staging table.
3. Within an atomic transaction, the target table is cleared and repopulated:
   ```sql
   DELETE FROM "{table}";
   INSERT INTO "{table}" SELECT * FROM "{staging_table}";
   DROP TABLE "{staging_table}";
   ```
4. If an error occurs at any point, the transaction rolls back, leaving the previous good target table **100% intact**.

### 3.5 Rejection Tracking & Quarantine Traceability
Invalid rows never crash the pipeline unless a batch-level schema error occurs. Instead, rows are partitioned into:
- **Clean Records:** Transition forward to persistence.
- **Quarantined Records:** Written to `rejected_records` with:
  - `run_id`: Lineage to the executing run.
  - `source_file`: Lineage to the originating raw file.
  - `row_number`: 1-indexed source row number.
  - `raw_payload`: Exact source line representation.
  - `reason_code`: Canonical code (`MALFORMED_ROW`, `MISSING_REQUIRED_FIELD`, `NULL_PRIMARY_KEY`, `DUPLICATE_KEY_IN_BATCH`, `DUPLICATE_KEY_TARGET`, `WRONG_DATA_TYPE`, `INVALID_CUSTOMER_FK`, `INVALID_PRODUCT_FK`, `INVALID_QUANTITY`, `NEGATIVE_PRICE`).
  - `reason_detail`: Descriptive diagnosis for debugging.

---

## 4. Mathematical Reconciliation Model

Every completed pipeline run guarantees two mathematical conservation laws:

$$\text{input\_count} = \text{accepted\_count} + \text{rejected\_count}$$

$$\text{accepted\_count} = \text{inserted\_count} + \text{updated\_count} + \text{skipped\_count}$$

These invariants are programmatically verified and monitored across all runs via `database.get_reconciliation_summary(conn)`.
