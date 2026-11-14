# ConfigFlow ETL – Design Trade-offs & Engineering Decisions

This document outlines key technical decisions made during the architecture of the ConfigFlow ETL platform, exploring the trade-offs, evaluated alternatives, and production recommendations.

---

## 1. Storage Engine: SQLite vs. DuckDB vs. Client-Server PostgreSQL

| Dimension | SQLite (Implemented) | DuckDB (Alternative) | PostgreSQL (Alternative) |
| :--- | :--- | :--- | :--- |
| **Footprint & Dependencies** | Built-in Python standard library (`sqlite3`); zero external C/Rust dependencies. | Lightweight embedded OLAP, requires external compiled wheel. | Client-server architecture requiring network daemon and credentials. |
| **Transaction Semantics** | ACID compliant with full rollback support and WAL mode. | Acid compliant, columnar storage optimized for analytical scans. | Enterprise-grade MVCC and distributed clustering. |
| **Suitability for Forward Deployed Hackathon** | **Optimal:** Instant single-command execution in any clean environment without Docker, external network access, or credentials. | Highly attractive for pure OLAP, but adds binary wheel dependency. | Heavyweight, requires containerization or local service management. |

*Decision:* SQLite was chosen because it delivers full relational integrity (`PRAGMA foreign_keys = ON`), robust ACID transactions, high write throughput via WAL mode, and zero friction in isolated testing environments.

---

## 2. In-Batch Deduplication Policy: First-Wins vs. Last-Wins vs. Abort

- **Option A (Abort on Duplicate):** Fails the entire batch if any duplicate key appears.
  - *Drawback:* Brittle in high-volume streaming or batched CDC where upstream micro-retries can produce benign duplicate keys.
- **Option B (Last-Wins):** Latest row in batch supersedes prior rows; earlier rows are discarded or quarantined.
  - *Advantage:* Naturally models change-data-capture (CDC) replay where later rows reflect newer states.
- **Option C (First-Wins, Implemented Default):** Earliest row in batch is retained; all subsequent duplicate occurrences are quarantined with `DUPLICATE_KEY_IN_BATCH`.
  - *Advantage:* Highly deterministic, simple to trace in quarantine logs, and preserves first-arrival causality unless explicit event timestamps dictate otherwise.
  - *Flexibility:* Configurable per dataset via the `"batch_dedup_policy"` metadata property (`first-wins` or `last-wins`).

---

## 3. Monetary Precision: Decimal vs. Floating-Point vs. Integer Cents

- **Option A (IEEE-754 Floating-Point `float`):**
  - *Severe Risk:* Binary floating-point drift (`0.10 * 3 = 0.30000000000000004`). Fails monetary boundary equality checks and audit balances.
- **Option B (Integer Cents):**
  - *Advantage:* Exact integer arithmetic; popular in payment gateways (e.g. Stripe).
  - *Drawback:* Requires extensive conversions when interfacing with external systems or fractional quantities/tax rates.
- **Option C (Python `decimal.Decimal` with `ROUND_HALF_UP`, Implemented):**
  - *Advantage:* Exact base-10 decimal representation. Prevents precision drift while maintaining transparent floating representation in SQL and reporting interfaces. Standard practice for enterprise ERP and accounting integrations.

---

## 4. Atomic Replace: In-Place Truncate vs. Shadow Staging Table

- **Option A (In-Place `DELETE FROM target` followed by Insert):**
  - *Severe Risk:* If the insert fails midway (e.g., unexpected data type or disk pressure), the target table is either left half-empty or completely lost if transaction handling is uncoordinated.
- **Option B (Shadow Staging Swap, Implemented):**
  - The incoming records are fully loaded and committed into a temporary staging table (`"{table}_staging_{run_id}"`).
  - An atomic transaction executes the swap:
    ```sql
    DELETE FROM "{table}";
    INSERT INTO "{table}" SELECT * FROM "{staging_table}";
    DROP TABLE "{staging_table}";
    ```
  - *Advantage:* True atomic durability. If any validation or database error occurs, rollback leaves the existing production target table completely intact.

---

## 5. Row-Level Rejection vs. Batch-Level Failure

A fundamental design question for Forward Deployed Engineers is: *When should a pipeline quarantine a row vs. abort the entire batch?*

- **Row-Level Quarantine (Implemented for Data Quality):**
  - Applied to individual corrupt rows: missing required fields, null primary keys, bad regex, negative prices, zero quantities, invalid foreign keys.
  - *Rationale:* In enterprise ingestions of 100,000+ records, rejecting 2 bad rows while persisting 99,998 good rows maintains business continuity, provided the 2 bad rows are durably quarantined with complete lineage.
- **Batch-Level Abort (Implemented for Structural Corruption):**
  - Applied to structural failures: invalid JSON syntax, CSV header missing expected columns, unexpected extra columns (when `allow_extra_columns: false`), database disk failures.
  - *Rationale:* A structural change signifies an uncoordinated upstream contract breach or system failure that invalidates the entire payload.
