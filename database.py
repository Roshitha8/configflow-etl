"""
database.py - SQLite Schema, Persistence, Upsert, Run Audit, Reject Tracking, and Queries.

Part of ConfigFlow ETL Platform.
Follows strict architectural constraints:
- Parameterized SQL execution exclusively.
- Strict SQL identifier validation against allowlists and regex.
- Foreign keys enforced (PRAGMA foreign_keys = ON).
- Atomic transaction management with rollback on failure.
- Separate durable audit logging for failed runs.
- Append, Upsert, and Atomic Replace load modes.
"""

from __future__ import annotations

import os
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Generator, Iterable, List, Optional, Set, Tuple

IDENTIFIER_REGEX = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
ALLOWED_TABLES = {
    "customers",
    "products",
    "orders",
    "pipeline_runs",
    "rejected_records",
    "order_change_history",
}

DEFAULT_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "warehouse.db")


def validate_identifier(name: str, allowed_set: Optional[Set[str]] = None) -> str:
    """
    Validate that an identifier is alphanumeric/underscore and matches allowed set.
    Prevents SQL injection via table or column names.
    """
    if not isinstance(name, str) or not IDENTIFIER_REGEX.match(name):
        raise ValueError(f"Invalid SQL identifier: '{name}'. Must match ^[a-zA-Z_][a-zA-Z0-9_]*$")
    if allowed_set is not None and name.lower() not in {s.lower() for s in allowed_set}:
        raise ValueError(f"Unauthorized SQL identifier: '{name}'. Not in allowed set: {allowed_set}")
    return name


def get_connection(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """
    Create a SQLite connection with foreign keys enabled and WAL mode.
    """
    # Ensure parent directory exists if not an in-memory db
    if db_path != ":memory:":
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)

    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    if db_path != ":memory:":
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection) -> Generator[sqlite3.Cursor, None, None]:
    """
    Context manager for atomic database transactions.
    Rolls back automatically on unhandled exceptions.
    """
    cursor = conn.cursor()
    try:
        yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def initialize_schema(conn: sqlite3.Connection) -> None:
    """
    Initialize all target tables, audit tables, indexes, and constraints.
    """
    ddl_statements = [
        # 1. Customers Target Table
        """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id TEXT PRIMARY KEY,
            customer_name TEXT NOT NULL,
            email TEXT,
            created_at TEXT NOT NULL,
            source_file TEXT NOT NULL,
            ingestion_run_id TEXT NOT NULL,
            ingested_at TEXT NOT NULL
        );
        """,
        # 2. Products Target Table
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id TEXT PRIMARY KEY,
            product_name TEXT NOT NULL,
            unit_price REAL NOT NULL CHECK(unit_price >= 0),
            active INTEGER NOT NULL CHECK(active IN (0, 1)),
            source_file TEXT NOT NULL,
            ingestion_run_id TEXT NOT NULL,
            ingested_at TEXT NOT NULL
        );
        """,
        # 3. Orders Target Table
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL REFERENCES customers(customer_id) ON DELETE RESTRICT,
            product_id TEXT NOT NULL REFERENCES products(product_id) ON DELETE RESTRICT,
            quantity INTEGER NOT NULL CHECK(quantity > 0),
            unit_price REAL NOT NULL CHECK(unit_price >= 0),
            order_timestamp TEXT NOT NULL,
            total_amount REAL NOT NULL CHECK(total_amount >= 0),
            source_file TEXT NOT NULL,
            ingestion_run_id TEXT NOT NULL,
            ingested_at TEXT NOT NULL
        );
        """,
        # 4. Pipeline Runs Audit Table
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id TEXT PRIMARY KEY,
            dataset TEXT NOT NULL,
            status TEXT NOT NULL,
            source_file TEXT NOT NULL,
            load_mode TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            input_count INTEGER DEFAULT 0,
            accepted_count INTEGER DEFAULT 0,
            rejected_count INTEGER DEFAULT 0,
            inserted_count INTEGER DEFAULT 0,
            updated_count INTEGER DEFAULT 0,
            skipped_count INTEGER DEFAULT 0,
            failure_reason TEXT,
            elapsed_seconds REAL DEFAULT 0.0
        );
        """,
        # 5. Rejected Records Quarantine Table
        """
        CREATE TABLE IF NOT EXISTS rejected_records (
            reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL REFERENCES pipeline_runs(run_id),
            dataset TEXT NOT NULL,
            source_file TEXT NOT NULL,
            row_number INTEGER,
            raw_payload TEXT NOT NULL,
            reason_code TEXT NOT NULL,
            reason_detail TEXT NOT NULL,
            rejected_at TEXT NOT NULL
        );
        """,
        # 6. Order Change History Table (for late arrivals and updates)
        """
        CREATE TABLE IF NOT EXISTS order_change_history (
            history_id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT NOT NULL,
            customer_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            old_quantity INTEGER,
            new_quantity INTEGER,
            old_unit_price REAL,
            new_unit_price REAL,
            old_total_amount REAL,
            new_total_amount REAL,
            old_order_timestamp TEXT,
            new_order_timestamp TEXT,
            change_type TEXT NOT NULL,
            ingestion_run_id TEXT NOT NULL,
            recorded_at TEXT NOT NULL
        );
        """,
        # Indexes for query performance and FK enforcement
        "CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);",
        "CREATE INDEX IF NOT EXISTS idx_orders_product ON orders(product_id);",
        "CREATE INDEX IF NOT EXISTS idx_orders_timestamp ON orders(order_timestamp);",
        "CREATE INDEX IF NOT EXISTS idx_rejected_run_id ON rejected_records(run_id);",
        "CREATE INDEX IF NOT EXISTS idx_runs_dataset ON pipeline_runs(dataset);",
        "CREATE INDEX IF NOT EXISTS idx_change_history_order ON order_change_history(order_id);",
    ]

    with transaction(conn) as cur:
        for stmt in ddl_statements:
            cur.execute(stmt)


# ============================================================================
# AUDIT AND REJECTION TRACKING
# ============================================================================


def create_pipeline_run(
    conn: sqlite3.Connection,
    run_id: str,
    dataset: str,
    source_file: str,
    load_mode: str,
    started_at: str,
) -> None:
    """
    Durably record the initiation of a pipeline run.
    """
    sql = """
    INSERT INTO pipeline_runs (
        run_id, dataset, status, source_file, load_mode, started_at
    ) VALUES (?, ?, 'RUNNING', ?, ?, ?);
    """
    with transaction(conn) as cur:
        cur.execute(sql, (run_id, dataset, source_file, load_mode, started_at))


def update_pipeline_run(
    conn: sqlite3.Connection,
    run_id: str,
    status: str,
    ended_at: str,
    input_count: int,
    accepted_count: int,
    rejected_count: int,
    inserted_count: int,
    updated_count: int,
    skipped_count: int,
    elapsed_seconds: float,
    failure_reason: Optional[str] = None,
) -> None:
    """
    Durably update run metrics upon completion or failure.
    Uses its own committed transaction.
    """
    sql = """
    UPDATE pipeline_runs SET
        status = ?,
        ended_at = ?,
        input_count = ?,
        accepted_count = ?,
        rejected_count = ?,
        inserted_count = ?,
        updated_count = ?,
        skipped_count = ?,
        elapsed_seconds = ?,
        failure_reason = ?
    WHERE run_id = ?;
    """
    with transaction(conn) as cur:
        cur.execute(
            sql,
            (
                status,
                ended_at,
                input_count,
                accepted_count,
                rejected_count,
                inserted_count,
                updated_count,
                skipped_count,
                elapsed_seconds,
                failure_reason,
                run_id,
            ),
        )


def record_rejected_records(
    conn: sqlite3.Connection,
    records: List[Dict[str, Any]],
) -> None:
    """
    Quarantine rejected records into the rejected_records table in batch.
    """
    if not records:
        return
    sql = """
    INSERT INTO rejected_records (
        run_id, dataset, source_file, row_number, raw_payload, reason_code, reason_detail, rejected_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """
    params = [
        (
            r["run_id"],
            r["dataset"],
            r["source_file"],
            r.get("row_number"),
            r["raw_payload"],
            r["reason_code"],
            r["reason_detail"],
            r["rejected_at"],
        )
        for r in records
    ]
    with transaction(conn) as cur:
        cur.executemany(sql, params)


# ============================================================================
# FOREIGN KEY AND UNIQUENESS QUERY HELPERS
# ============================================================================


def get_existing_primary_keys(
    conn: sqlite3.Connection,
    table_name: str,
    pk_column: str,
) -> Set[Any]:
    """
    Retrieve all existing primary keys from a target table for pre-validation.
    """
    validate_identifier(table_name, ALLOWED_TABLES)
    validate_identifier(pk_column)
    sql = f'SELECT "{pk_column}" FROM "{table_name}";'
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        return {row[0] for row in cursor.fetchall()}
    finally:
        cursor.close()


def check_foreign_keys_bulk(
    conn: sqlite3.Connection,
    target_table: str,
    target_col: str,
    candidate_keys: Iterable[Any],
) -> Set[Any]:
    """
    Given a set of candidate foreign keys, return the subset that actually exists in the target table.
    """
    validate_identifier(target_table, ALLOWED_TABLES)
    validate_identifier(target_col)
    keys_list = list(set(candidate_keys))
    if not keys_list:
        return set()

    found_keys = set()
    # Query in chunks to avoid SQLite variable limit (default 999 or 32766)
    chunk_size = 500
    cursor = conn.cursor()
    try:
        for i in range(0, len(keys_list), chunk_size):
            chunk = keys_list[i : i + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            sql = f'SELECT "{target_col}" FROM "{target_table}" WHERE "{target_col}" IN ({placeholders});'
            cursor.execute(sql, chunk)
            found_keys.update(row[0] for row in cursor.fetchall())
    finally:
        cursor.close()
    return found_keys


def get_existing_records_map(
    conn: sqlite3.Connection,
    table_name: str,
    pk_column: str,
    candidate_keys: Iterable[Any],
) -> Dict[Any, Dict[str, Any]]:
    """
    Retrieve full existing records for specific candidate keys to compare during upsert.
    """
    validate_identifier(table_name, ALLOWED_TABLES)
    validate_identifier(pk_column)
    keys_list = list(set(candidate_keys))
    if not keys_list:
        return {}

    records_map = {}
    chunk_size = 500
    cursor = conn.cursor()
    try:
        for i in range(0, len(keys_list), chunk_size):
            chunk = keys_list[i : i + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            sql = f'SELECT * FROM "{table_name}" WHERE "{pk_column}" IN ({placeholders});'
            cursor.execute(sql, chunk)
            for row in cursor.fetchall():
                row_dict = dict(row)
                records_map[row_dict[pk_column]] = row_dict
    finally:
        cursor.close()
    return records_map


# ============================================================================
# PERSISTENCE ENGINE: APPEND, UPSERT, AND ATOMIC REPLACE
# ============================================================================


def persist_records(
    conn: sqlite3.Connection,
    table_name: str,
    load_mode: str,
    records: List[Dict[str, Any]],
    pk_columns: List[str],
    column_names: List[str],
    run_id: str,
    source_file: str,
    allow_row_rejects_on_replace: bool = True,
) -> Tuple[int, int, int, List[Dict[str, Any]]]:
    """
    Execute transactional persistence for the given load mode.

    Returns:
        (inserted_count, updated_count, skipped_count, db_rejected_records)
    """
    validate_identifier(table_name, ALLOWED_TABLES)
    for col in column_names:
        validate_identifier(col)
    for pk in pk_columns:
        validate_identifier(pk)

    mode = load_mode.lower()
    if mode not in {"append", "upsert", "replace"}:
        raise ValueError(f"Unsupported load mode: '{load_mode}'. Allowed: append, upsert, replace.")

    if not records:
        return (0, 0, 0, [])

    if mode == "append":
        return _persist_append(conn, table_name, records, pk_columns, column_names, run_id, source_file)
    elif mode == "upsert":
        return _persist_upsert(conn, table_name, records, pk_columns, column_names, run_id, source_file)
    elif mode == "replace":
        return _persist_replace(
            conn,
            table_name,
            records,
            pk_columns,
            column_names,
            run_id,
            source_file,
            allow_row_rejects=allow_row_rejects_on_replace,
        )
    else:
        raise ValueError(f"Unknown mode: {mode}")


def _persist_append(
    conn: sqlite3.Connection,
    table_name: str,
    records: List[Dict[str, Any]],
    pk_columns: List[str],
    column_names: List[str],
    run_id: str,
    source_file: str,
) -> Tuple[int, int, int, List[Dict[str, Any]]]:
    """
    Append Mode:
    - Rejects duplicate business keys that already exist in target table.
    - Inserts all unique non-conflicting records.
    """
    pk_col = pk_columns[0]
    candidate_keys = [r[pk_col] for r in records]
    existing_keys = get_existing_primary_keys(conn, table_name, pk_col)

    to_insert: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []

    now_iso = datetime.now(timezone.utc).isoformat()
    for rec in records:
        key = rec[pk_col]
        if key in existing_keys:
            rejected.append(
                {
                    "run_id": run_id,
                    "dataset": table_name,
                    "source_file": source_file,
                    "row_number": rec.get("_row_number"),
                    "raw_payload": str(rec),
                    "reason_code": "DUPLICATE_KEY_TARGET",
                    "reason_detail": f"Record with primary key '{key}' already exists in target table '{table_name}'. Append rejected duplicate business key.",
                    "rejected_at": now_iso,
                }
            )
        else:
            to_insert.append(rec)

    if to_insert:
        cols_sql = ", ".join(f'"{col}"' for col in column_names)
        placeholders = ", ".join("?" for _ in column_names)
        insert_sql = f'INSERT INTO "{table_name}" ({cols_sql}) VALUES ({placeholders});'
        params = [tuple(r[col] for col in column_names) for r in to_insert]
        with transaction(conn) as cur:
            cur.executemany(insert_sql, params)

    return (len(to_insert), 0, 0, rejected)


def _persist_upsert(
    conn: sqlite3.Connection,
    table_name: str,
    records: List[Dict[str, Any]],
    pk_columns: List[str],
    column_names: List[str],
    run_id: str,
    source_file: str,
) -> Tuple[int, int, int, List[Dict[str, Any]]]:
    """
    Upsert Mode:
    - If PK not in DB: INSERT.
    - If PK in DB and values unchanged: SKIP.
    - If PK in DB and values changed:
        - Check late-arriving timestamps if applicable (e.g. order_timestamp).
        - If table is orders, record old and new state in order_change_history.
        - UPDATE target row.
    """
    pk_col = pk_columns[0]
    candidate_keys = [r[pk_col] for r in records]
    existing_map = get_existing_records_map(conn, table_name, pk_col, candidate_keys)

    to_insert: List[Dict[str, Any]] = []
    to_update: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []  # (incoming, existing)
    skipped_count = 0
    change_history_entries: List[Dict[str, Any]] = []
    now_iso = datetime.now(timezone.utc).isoformat()

    # Compare non-audit columns to determine change
    audit_cols = {"source_file", "ingestion_run_id", "ingested_at"}
    data_cols = [c for c in column_names if c not in audit_cols and c != pk_col]

    for rec in records:
        key = rec[pk_col]
        if key not in existing_map:
            to_insert.append(rec)
        else:
            existing = existing_map[key]
            # Check if any substantive data column changed
            is_changed = False
            for col in data_cols:
                val_new = rec.get(col)
                val_old = existing.get(col)

                # Normalize float/decimal comparisons
                if isinstance(val_new, (float, Decimal)) or isinstance(val_old, (float, Decimal)):
                    if abs(float(val_new) - float(val_old)) > 1e-6:
                        is_changed = True
                        break
                elif val_new != val_old:
                    is_changed = True
                    break

            if not is_changed:
                skipped_count += 1
            else:
                # Value changed - check event timestamp for late/out-of-order events
                if "order_timestamp" in rec and "order_timestamp" in existing:
                    new_ts = str(rec["order_timestamp"])
                    old_ts = str(existing["order_timestamp"])
                    # If incoming record is older than existing record, preserve existing latest state
                    # but record the late arrival in change history
                    if new_ts < old_ts:
                        change_history_entries.append(
                            {
                                "order_id": key,
                                "customer_id": rec.get("customer_id", existing.get("customer_id")),
                                "product_id": rec.get("product_id", existing.get("product_id")),
                                "old_quantity": existing.get("quantity"),
                                "new_quantity": rec.get("quantity"),
                                "old_unit_price": existing.get("unit_price"),
                                "new_unit_price": rec.get("unit_price"),
                                "old_total_amount": existing.get("total_amount"),
                                "new_total_amount": rec.get("total_amount"),
                                "old_order_timestamp": old_ts,
                                "new_order_timestamp": new_ts,
                                "change_type": "LATE_EVENT_RECORDED",
                                "ingestion_run_id": run_id,
                                "recorded_at": now_iso,
                            }
                        )
                        skipped_count += 1
                        continue

                to_update.append((rec, existing))
                if table_name == "orders":
                    change_history_entries.append(
                        {
                            "order_id": key,
                            "customer_id": rec.get("customer_id", existing.get("customer_id")),
                            "product_id": rec.get("product_id", existing.get("product_id")),
                            "old_quantity": existing.get("quantity"),
                            "new_quantity": rec.get("quantity"),
                            "old_unit_price": existing.get("unit_price"),
                            "new_unit_price": rec.get("unit_price"),
                            "old_total_amount": existing.get("total_amount"),
                            "new_total_amount": rec.get("total_amount"),
                            "old_order_timestamp": existing.get("order_timestamp"),
                            "new_order_timestamp": rec.get("order_timestamp"),
                            "change_type": "UPDATE",
                            "ingestion_run_id": run_id,
                            "recorded_at": now_iso,
                        }
                    )

    with transaction(conn) as cur:
        # Perform Inserts
        if to_insert:
            cols_sql = ", ".join(f'"{col}"' for col in column_names)
            placeholders = ", ".join("?" for _ in column_names)
            insert_sql = f'INSERT INTO "{table_name}" ({cols_sql}) VALUES ({placeholders});'
            params = [tuple(r[col] for col in column_names) for r in to_insert]
            cur.executemany(insert_sql, params)

        # Perform Updates
        if to_update:
            update_cols = [c for c in column_names if c != pk_col]
            set_clause = ", ".join(f'"{c}" = ?' for c in update_cols)
            update_sql = f'UPDATE "{table_name}" SET {set_clause} WHERE "{pk_col}" = ?;'
            params = [tuple([rec[c] for c in update_cols] + [rec[pk_col]]) for rec, _ in to_update]
            cur.executemany(update_sql, params)

        # Record Change History
        if change_history_entries:
            hist_sql = """
            INSERT INTO order_change_history (
                order_id, customer_id, product_id,
                old_quantity, new_quantity, old_unit_price, new_unit_price,
                old_total_amount, new_total_amount, old_order_timestamp, new_order_timestamp,
                change_type, ingestion_run_id, recorded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """
            hist_params = [
                (
                    h["order_id"],
                    h["customer_id"],
                    h["product_id"],
                    h["old_quantity"],
                    h["new_quantity"],
                    h["old_unit_price"],
                    h["new_unit_price"],
                    h["old_total_amount"],
                    h["new_total_amount"],
                    h["old_order_timestamp"],
                    h["new_order_timestamp"],
                    h["change_type"],
                    h["ingestion_run_id"],
                    h["recorded_at"],
                )
                for h in change_history_entries
            ]
            cur.executemany(hist_sql, hist_params)

    return (len(to_insert), len(to_update), skipped_count, [])


def _persist_replace(
    conn: sqlite3.Connection,
    table_name: str,
    records: List[Dict[str, Any]],
    pk_columns: List[str],
    column_names: List[str],
    run_id: str,
    source_file: str,
    allow_row_rejects: bool = True,
) -> Tuple[int, int, int, List[Dict[str, Any]]]:
    """
    Atomic Replace Mode:
    - Loads clean accepted records into a temporary staging table.
    - Inside a single atomic transaction:
        - Inserts or updates existing target records matching replacement batch.
        - Prunes old records not in the replacement batch.
        - Preserves foreign-key referential integrity (ON DELETE RESTRICT) for active entities.
    - If any database failure occurs, the transaction rolls back, preserving
      the previous good target table unchanged.
    """
    staging_table = f"{table_name}_staging_{run_id.replace('-', '_')}"
    validate_identifier(table_name, ALLOWED_TABLES)

    cols_sql = ", ".join(f'"{col}"' for col in column_names)
    placeholders = ", ".join("?" for _ in column_names)

    pk_col = pk_columns[0] if pk_columns else None
    if pk_col:
        validate_identifier(pk_col)
        non_pk_cols = [c for c in column_names if c != pk_col]
        update_clause = ", ".join(f'"{c}" = excluded."{c}"' for c in non_pk_cols)
    else:
        update_clause = None

    # Use atomic transaction
    with transaction(conn) as cur:
        # Create staging table with identical schema
        cur.execute(f'CREATE TEMP TABLE "{staging_table}" AS SELECT * FROM "{table_name}" WHERE 0 = 1;')

        # Insert accepted records into staging table
        insert_staging_sql = f'INSERT INTO "{staging_table}" ({cols_sql}) VALUES ({placeholders});'
        params = [tuple(r[col] for col in column_names) for r in records]
        cur.executemany(insert_staging_sql, params)

        if pk_col and update_clause:
            # Atomic upsert from staging table into target table
            cur.execute(
                f'INSERT INTO "{table_name}" ({cols_sql}) SELECT {cols_sql} FROM "{staging_table}" '
                f'WHERE true ON CONFLICT("{pk_col}") DO UPDATE SET {update_clause};'
            )
            # Prune obsolete rows not present in incoming replacement batch
            cur.execute(f'DELETE FROM "{table_name}" WHERE "{pk_col}" NOT IN (SELECT "{pk_col}" FROM "{staging_table}");')
        else:
            # Fallback for tables without non-PK updates
            cur.execute(f'DELETE FROM "{table_name}";')
            cur.execute(f'INSERT INTO "{table_name}" ({cols_sql}) SELECT {cols_sql} FROM "{staging_table}";')

        cur.execute(f'DROP TABLE "{staging_table}";')

    return (len(records), 0, 0, [])


# ============================================================================
# QUERY INTERFACE FOR FRONTEND AND VERIFICATION
# ============================================================================


def get_table_count(conn: sqlite3.Connection, table_name: str) -> int:
    """Return total row count of any allowed table."""
    validate_identifier(table_name, ALLOWED_TABLES)
    cursor = conn.cursor()
    try:
        cursor.execute(f'SELECT COUNT(*) FROM "{table_name}";')
        return cursor.fetchone()[0]
    finally:
        cursor.close()


def get_table_preview(
    conn: sqlite3.Connection,
    table_name: str,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Return top N rows from a table as dictionaries."""
    validate_identifier(table_name, ALLOWED_TABLES)
    cursor = conn.cursor()
    try:
        cursor.execute(f'SELECT * FROM "{table_name}" LIMIT ?;', (limit,))
        return [dict(row) for row in cursor.fetchall()]
    finally:
        cursor.close()


def get_pipeline_runs(
    conn: sqlite3.Connection,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Return recent pipeline execution records."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT * FROM pipeline_runs
            ORDER BY started_at DESC
            LIMIT ?;
            """,
            (limit,),
        )
        return [dict(row) for row in cursor.fetchall()]
    finally:
        cursor.close()


def get_pipeline_run(
    conn: sqlite3.Connection,
    run_id: str,
) -> Optional[Dict[str, Any]]:
    """Retrieve details for a single pipeline run."""
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM pipeline_runs WHERE run_id = ?;", (run_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    finally:
        cursor.close()


def get_rejected_records(
    conn: sqlite3.Connection,
    run_id: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Return quarantined rejected records, optionally filtered by run_id."""
    cursor = conn.cursor()
    try:
        if run_id:
            cursor.execute(
                """
                SELECT * FROM rejected_records
                WHERE run_id = ?
                ORDER BY reject_id ASC
                LIMIT ?;
                """,
                (run_id, limit),
            )
        else:
            cursor.execute(
                """
                SELECT * FROM rejected_records
                ORDER BY reject_id DESC
                LIMIT ?;
                """,
                (limit,),
            )
        return [dict(row) for row in cursor.fetchall()]
    finally:
        cursor.close()


def get_order_change_history(
    conn: sqlite3.Connection,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Retrieve recent order change history entries."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT * FROM order_change_history
            ORDER BY recorded_at DESC
            LIMIT ?;
            """,
            (limit,),
        )
        return [dict(row) for row in cursor.fetchall()]
    finally:
        cursor.close()


def get_reconciliation_summary(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """
    Produce an overall reconciliation summary across all pipeline runs.
    Verifies invariants:
    1. input_count = accepted_count + rejected_count
    2. accepted_count = inserted_count + updated_count + skipped_count (for upsert/append)
    """
    runs = get_pipeline_runs(conn, limit=1000)
    summary = []
    for r in runs:
        inp = r["input_count"] or 0
        acc = r["accepted_count"] or 0
        rej = r["rejected_count"] or 0
        ins = r["inserted_count"] or 0
        upd = r["updated_count"] or 0
        skp = r["skipped_count"] or 0

        # Invariant 1: Total input equals accepted plus rejected
        inv1_passed = (inp == acc + rej) or (r["status"] == "FAILED")
        # Invariant 2: Total accepted equals inserted + updated + skipped
        inv2_passed = (acc == ins + upd + skp) or (r["status"] == "FAILED")

        summary.append(
            {
                "run_id": r["run_id"],
                "dataset": r["dataset"],
                "status": r["status"],
                "load_mode": r["load_mode"],
                "input_count": inp,
                "accepted_count": acc,
                "rejected_count": rej,
                "inserted_count": ins,
                "updated_count": upd,
                "skipped_count": skp,
                "inv1_input_reconciled": inv1_passed,
                "inv2_accepted_reconciled": inv2_passed,
                "all_reconciled": inv1_passed and inv2_passed,
            }
        )
    return summary
