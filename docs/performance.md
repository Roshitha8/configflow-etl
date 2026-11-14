# ConfigFlow ETL – Performance Benchmarks & Empirical Analysis

This document presents empirical performance measurements, hardware profiling, database indexing strategies, and scaling optimizations for the ConfigFlow ETL platform.

---

## 1. Empirical Benchmark Results

All figures below reflect **actual observed execution measurements** on the target test environment.

### 1.1 Test Environment & Hardware Context
- **Operating System:** Windows 10 (Build 10.0.26200)
- **CPU Architecture:** Intel64 Family 6 Model 140 Stepping 1, GenuineIntel
- **Cores:** 8 logical cores
- **Python Version:** 3.10.6 (64-bit)
- **Database Engine:** SQLite 3.39+ with WAL journal mode (`PRAGMA journal_mode = WAL`)

---

### 1.2 Workload Comparison

| Metric | Starter Baseline Run | High-Volume Benchmark Run |
| :--- | :--- | :--- |
| **Datasets Ingested** | 3 (`customers`, `products`, `orders`) | 3 (`customers`, `products`, `orders`) |
| **Total Ingested Records** | 170 rows | **101,100 rows** |
| - Customers | 50 rows | 1,000 rows |
| - Products | 20 rows | 100 rows |
| - Orders | 100 rows | 100,000 rows |
| **Orders Ingestion Time** | 0.0130 seconds | **5.1186 seconds** |
| **Orders Throughput** | 7,692 rows/sec | **19,536.59 rows/sec** |
| **Total Pipeline Batch Duration** | 0.0540 seconds | **5.2065 seconds** |
| **Aggregate Throughput** | 3,149.49 records/sec | **19,418.04 records/sec** |
| **Quarantined Rejects** | 0 rows | 0 rows |
| **Reconciliation Status** | **100% Invariants Passed** | **100% Invariants Passed** |

---

## 2. Architectural Performance Drivers

The high ingestion throughput (~19,500 rows/second for end-to-end validation, transformation, foreign-key checking, and SQLite insertion) is driven by four architectural optimizations:

### 2.1 Parameterized Batch Execution (`executemany`)
Row-by-row `INSERT` operations suffer from repeated query compilation and transaction overhead. ConfigFlow uses Python's `sqlite3.Cursor.executemany` with pre-compiled parameterized queries, reducing per-row driver overhead to microsecond latency.

### 2.2 Write-Ahead Logging (WAL Mode)
```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
```
WAL mode permits concurrent readers without blocking writes, avoids disk fsync calls on every transaction boundary, and turns random writes into sequential log appends.

### 2.3 Chunked Bulk Foreign Key Pre-Checking
Rather than validating foreign keys row-by-row across 100,000 queries, ConfigFlow extracts distinct candidate foreign keys and queries target parent tables in chunks:
```sql
SELECT "customer_id" FROM "customers" WHERE "customer_id" IN (?, ?, ...);
```
This reduces 100,000 individual roundtrips to just 2 indexed bulk queries.

### 2.4 Targeted B-Tree Indexing
Foreign key and query lookup fields are indexed explicitly in `database.py`:
- `CREATE INDEX idx_orders_customer ON orders(customer_id);`
- `CREATE INDEX idx_orders_product ON orders(product_id);`
- `CREATE INDEX idx_orders_timestamp ON orders(order_timestamp);`
- `CREATE INDEX idx_rejected_run_id ON rejected_records(run_id);`
- `CREATE INDEX idx_change_history_order ON order_change_history(order_id);`

---

## 3. Plausible Production Optimization Roadmap

For enterprise scaling beyond 10,000,000+ daily orders:
1. **Chunked Streaming Iterators:** Read large CSV/JSON files in streaming chunks (e.g. `pd.read_csv(chunksize=50000)` or `csv.reader` generator yields) to cap memory footprint to under 100MB regardless of file size.
2. **Worker Process Parallelism:** Validate and transform non-overlapping file partitions across multiple Python `multiprocessing` worker pools before a single transactional commit.
3. **DuckDB / Polars In-Memory Arrow Engine:** Utilize Apache Arrow / Polars for vectorized columnar data quality validations prior to relational warehouse persistence.
