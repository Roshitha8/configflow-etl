"""
tests/test_pipeline.py - Exhaustive Automated Test Suite for ConfigFlow ETL Platform.

Covers all 28 required test scenarios:
 1. Valid CSV ingestion
 2. Valid JSON ingestion
 3. Empty batch
 4. Malformed CSV
 5. Malformed JSON
 6. Missing required field
 7. Null primary key
 8. Duplicate source key
 9. Duplicate target key
10. Unexpected extra schema column
11. Missing schema column
12. Wrong data type
13. Invalid customer foreign key
14. Invalid product foreign key
15. Zero and negative quantity
16. Negative price
17. Boundary monetary values
18. Late/out-of-order events
19. Identical-input rerun idempotency
20. Upsert changed record
21. Append semantics
22. Atomic replace
23. Injected partial database failure and retry
24. Reconciliation invariants
25. Audit log correctness
26. Quarantine traceability
27. Config validation and unsafe identifiers
28. Large data generation determinism
"""

import json
import os
import sqlite3
import pytest
from decimal import Decimal

# Aliases for JSON-like booleans in test dictionaries
false = False
true = True

import backend
import database
from scripts import generate_data


# ============================================================================
# TEST HELPERS AND FIXTURES
# ============================================================================


def create_test_config(
    tmp_path,
    dataset_name: str,
    source_file: str,
    source_format: str,
    target_table: str,
    load_mode: str,
    primary_key: list,
    columns: dict,
    required_fields: list = None,
    foreign_keys: list = None,
    transformations: list = None,
    validation_rules: list = None,
    allow_extra_columns: bool = False,
    dependencies: list = None,
) -> str:
    """Helper to create an isolated JSON config file for testing."""
    config = {
        "schema_version": "1.0.0",
        "dataset_name": dataset_name,
        "source": {
            "path": source_file,
            "format": source_format,
        },
        "target": {
            "table": target_table,
            "load_mode": load_mode,
        },
        "primary_key": primary_key,
        "columns": columns,
        "required_fields": required_fields or [],
        "foreign_keys": foreign_keys or [],
        "transformations": transformations or [],
        "validation_rules": validation_rules or [],
        "allow_extra_columns": allow_extra_columns,
        "batch_dedup_policy": "first-wins",
        "dependencies": dependencies or [],
    }
    cfg_path = os.path.join(tmp_path, f"{dataset_name}_config.json")
    with open(cfg_path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)
    return cfg_path


# ============================================================================
# 28 EXHAUSTIVE TEST IMPLEMENTATIONS
# ============================================================================


def test_01_valid_csv_ingestion(tmp_path):
    """1. Valid CSV ingestion."""
    db_path = str(tmp_path / "test.db")
    csv_file = str(tmp_path / "customers.csv")
    with open(csv_file, "w", encoding="utf-8") as f:
        f.write("customer_id,customer_name,email,created_at\n")
        f.write("CUST-00001,Alice Smith,alice@example.com,2026-01-01T10:00:00Z\n")
        f.write("CUST-00002,Bob Jones,bob@example.com,2026-01-01T10:05:00Z\n")

    cfg_path = create_test_config(
        tmp_path,
        dataset_name="customers",
        source_file=csv_file,
        source_format="csv",
        target_table="customers",
        load_mode="upsert",
        primary_key=["customer_id"],
        columns={
            "customer_id": {"type": "string", "nullable": false},
            "customer_name": {"type": "string", "nullable": false},
            "email": {"type": "string", "nullable": true},
            "created_at": {"type": "timestamp", "nullable": false},
        },
        required_fields=["customer_id", "customer_name", "created_at"],
    )

    res = backend.run_pipeline(cfg_path, db_path=db_path)
    assert res["status"] == "SUCCESS"
    assert res["input_count"] == 2
    assert res["accepted_count"] == 2
    assert res["inserted_count"] == 2
    assert res["rejected_count"] == 0

    conn = database.get_connection(db_path)
    assert database.get_table_count(conn, "customers") == 2
    conn.close()


def test_02_valid_json_ingestion(tmp_path):
    """2. Valid JSON ingestion."""
    db_path = str(tmp_path / "test.db")
    json_file = str(tmp_path / "products.json")
    products = [
        {"product_id": "PROD-00001", "product_name": "Edge Switch", "unit_price": 49.99, "active": True},
        {"product_id": "PROD-00002", "product_name": "Sensor Node", "unit_price": 19.50, "active": False},
    ]
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(products, f)

    cfg_path = create_test_config(
        tmp_path,
        dataset_name="products",
        source_file=json_file,
        source_format="json",
        target_table="products",
        load_mode="upsert",
        primary_key=["product_id"],
        columns={
            "product_id": {"type": "string", "nullable": false},
            "product_name": {"type": "string", "nullable": false},
            "unit_price": {"type": "decimal", "nullable": false},
            "active": {"type": "boolean", "nullable": false},
        },
        required_fields=["product_id", "product_name", "unit_price", "active"],
        validation_rules=[{"column": "unit_price", "rule": "min_value", "min": "0.00"}],
    )

    res = backend.run_pipeline(cfg_path, db_path=db_path)
    assert res["status"] == "SUCCESS"
    assert res["input_count"] == 2
    assert res["accepted_count"] == 2
    assert res["inserted_count"] == 2
    assert res["rejected_count"] == 0

    conn = database.get_connection(db_path)
    assert database.get_table_count(conn, "products") == 2
    conn.close()


def test_03_empty_batch(tmp_path):
    """3. Empty batch (0-byte CSV)."""
    db_path = str(tmp_path / "test.db")
    csv_file = str(tmp_path / "empty.csv")
    with open(csv_file, "w", encoding="utf-8") as f:
        pass  # 0 bytes

    cfg_path = create_test_config(
        tmp_path,
        dataset_name="customers",
        source_file=csv_file,
        source_format="csv",
        target_table="customers",
        load_mode="upsert",
        primary_key=["customer_id"],
        columns={
            "customer_id": {"type": "string", "nullable": false},
            "customer_name": {"type": "string", "nullable": false},
            "email": {"type": "string", "nullable": true},
            "created_at": {"type": "timestamp", "nullable": false},
        },
    )

    res = backend.run_pipeline(cfg_path, db_path=db_path)
    assert res["status"] == "SUCCESS"
    assert res["input_count"] == 0
    assert res["accepted_count"] == 0
    assert res["rejected_count"] == 0


def test_04_malformed_csv(tmp_path):
    """4. Malformed CSV (row has fewer columns than header)."""
    db_path = str(tmp_path / "test.db")
    csv_file = str(tmp_path / "malformed.csv")
    with open(csv_file, "w", encoding="utf-8") as f:
        f.write("customer_id,customer_name,email,created_at\n")
        f.write("CUST-00001,Valid Name,valid@example.com,2026-01-01T10:00:00Z\n")
        f.write("CUST-00002,TooFewColumns\n")

    cfg_path = create_test_config(
        tmp_path,
        dataset_name="customers",
        source_file=csv_file,
        source_format="csv",
        target_table="customers",
        load_mode="upsert",
        primary_key=["customer_id"],
        columns={
            "customer_id": {"type": "string", "nullable": false},
            "customer_name": {"type": "string", "nullable": false},
            "email": {"type": "string", "nullable": true},
            "created_at": {"type": "timestamp", "nullable": false},
        },
    )

    res = backend.run_pipeline(cfg_path, db_path=db_path)
    assert res["status"] == "PARTIAL_SUCCESS"
    assert res["input_count"] == 2
    assert res["accepted_count"] == 1
    assert res["rejected_count"] == 1

    conn = database.get_connection(db_path)
    rejects = database.get_rejected_records(conn, run_id=res["run_id"])
    assert len(rejects) == 1
    assert rejects[0]["reason_code"] == "MALFORMED_ROW"
    conn.close()


def test_05_malformed_json(tmp_path):
    """5. Malformed JSON (syntax error causing safe batch failure)."""
    db_path = str(tmp_path / "test.db")
    json_file = str(tmp_path / "malformed.json")
    with open(json_file, "w", encoding="utf-8") as f:
        f.write("[{'product_id': broken syntax")

    cfg_path = create_test_config(
        tmp_path,
        dataset_name="products",
        source_file=json_file,
        source_format="json",
        target_table="products",
        load_mode="upsert",
        primary_key=["product_id"],
        columns={
            "product_id": {"type": "string", "nullable": false},
            "product_name": {"type": "string", "nullable": false},
            "unit_price": {"type": "decimal", "nullable": false},
            "active": {"type": "boolean", "nullable": false},
        },
    )

    res = backend.run_pipeline(cfg_path, db_path=db_path)
    assert res["status"] == "FAILED"
    assert "Malformed JSON" in res["failure_reason"]


def test_06_missing_required_field(tmp_path):
    """6. Missing required field (non-nullable customer_name is empty)."""
    db_path = str(tmp_path / "test.db")
    csv_file = str(tmp_path / "missing_req.csv")
    with open(csv_file, "w", encoding="utf-8") as f:
        f.write("customer_id,customer_name,email,created_at\n")
        f.write("CUST-00001,,user@example.com,2026-01-01T10:00:00Z\n")

    cfg_path = create_test_config(
        tmp_path,
        dataset_name="customers",
        source_file=csv_file,
        source_format="csv",
        target_table="customers",
        load_mode="upsert",
        primary_key=["customer_id"],
        columns={
            "customer_id": {"type": "string", "nullable": false},
            "customer_name": {"type": "string", "nullable": false},
            "email": {"type": "string", "nullable": true},
            "created_at": {"type": "timestamp", "nullable": false},
        },
        required_fields=["customer_id", "customer_name", "created_at"],
    )

    res = backend.run_pipeline(cfg_path, db_path=db_path)
    assert res["status"] == "PARTIAL_SUCCESS"
    assert res["rejected_count"] == 1

    conn = database.get_connection(db_path)
    rejects = database.get_rejected_records(conn, run_id=res["run_id"])
    assert rejects[0]["reason_code"] == "MISSING_REQUIRED_FIELD"
    conn.close()


def test_07_null_primary_key(tmp_path):
    """7. Null primary key."""
    db_path = str(tmp_path / "test.db")
    csv_file = str(tmp_path / "null_pk.csv")
    with open(csv_file, "w", encoding="utf-8") as f:
        f.write("customer_id,customer_name,email,created_at\n")
        f.write(",John Doe,user@example.com,2026-01-01T10:00:00Z\n")

    cfg_path = create_test_config(
        tmp_path,
        dataset_name="customers",
        source_file=csv_file,
        source_format="csv",
        target_table="customers",
        load_mode="upsert",
        primary_key=["customer_id"],
        columns={
            "customer_id": {"type": "string", "nullable": false},
            "customer_name": {"type": "string", "nullable": false},
            "email": {"type": "string", "nullable": true},
            "created_at": {"type": "timestamp", "nullable": false},
        },
    )

    res = backend.run_pipeline(cfg_path, db_path=db_path)
    assert res["status"] == "PARTIAL_SUCCESS"
    assert res["rejected_count"] == 1

    conn = database.get_connection(db_path)
    rejects = database.get_rejected_records(conn, run_id=res["run_id"])
    assert rejects[0]["reason_code"] == "NULL_PRIMARY_KEY"
    conn.close()


def test_08_duplicate_source_key(tmp_path):
    """8. Duplicate source key within batch (first-wins keeps 1st, quarantines 2nd)."""
    db_path = str(tmp_path / "test.db")
    csv_file = str(tmp_path / "dup_key.csv")
    with open(csv_file, "w", encoding="utf-8") as f:
        f.write("customer_id,customer_name,email,created_at\n")
        f.write("CUST-00001,Alice Smith,alice@example.com,2026-01-01T10:00:00Z\n")
        f.write("CUST-00001,Alice Duplicate,alice_dup@example.com,2026-01-01T10:05:00Z\n")

    cfg_path = create_test_config(
        tmp_path,
        dataset_name="customers",
        source_file=csv_file,
        source_format="csv",
        target_table="customers",
        load_mode="upsert",
        primary_key=["customer_id"],
        columns={
            "customer_id": {"type": "string", "nullable": false},
            "customer_name": {"type": "string", "nullable": false},
            "email": {"type": "string", "nullable": true},
            "created_at": {"type": "timestamp", "nullable": false},
        },
    )

    res = backend.run_pipeline(cfg_path, db_path=db_path)
    assert res["status"] == "PARTIAL_SUCCESS"
    assert res["input_count"] == 2
    assert res["accepted_count"] == 1
    assert res["rejected_count"] == 1

    conn = database.get_connection(db_path)
    rejects = database.get_rejected_records(conn, run_id=res["run_id"])
    assert rejects[0]["reason_code"] == "DUPLICATE_KEY_IN_BATCH"
    conn.close()


def test_09_duplicate_target_key(tmp_path):
    """9. Duplicate target key in append mode."""
    db_path = str(tmp_path / "test.db")
    csv_file1 = str(tmp_path / "batch1.csv")
    csv_file2 = str(tmp_path / "batch2.csv")

    with open(csv_file1, "w", encoding="utf-8") as f:
        f.write("customer_id,customer_name,email,created_at\n")
        f.write("CUST-00001,Alice Smith,alice@example.com,2026-01-01T10:00:00Z\n")

    with open(csv_file2, "w", encoding="utf-8") as f:
        f.write("customer_id,customer_name,email,created_at\n")
        f.write("CUST-00001,Alice Smith,alice@example.com,2026-01-01T10:00:00Z\n")

    cfg_path = create_test_config(
        tmp_path,
        dataset_name="customers",
        source_file=csv_file1,
        source_format="csv",
        target_table="customers",
        load_mode="append",
        primary_key=["customer_id"],
        columns={
            "customer_id": {"type": "string", "nullable": false},
            "customer_name": {"type": "string", "nullable": false},
            "email": {"type": "string", "nullable": true},
            "created_at": {"type": "timestamp", "nullable": false},
        },
    )

    res1 = backend.run_pipeline(cfg_path, db_path=db_path)
    assert res1["status"] == "SUCCESS"
    assert res1["inserted_count"] == 1

    # Rerun in append mode with existing key
    res2 = backend.run_pipeline(cfg_path, db_path=db_path, override_source_path=csv_file2)
    assert res2["status"] == "PARTIAL_SUCCESS"
    assert res2["inserted_count"] == 0
    assert res2["rejected_count"] == 1

    conn = database.get_connection(db_path)
    rejects = database.get_rejected_records(conn, run_id=res2["run_id"])
    assert rejects[0]["reason_code"] == "DUPLICATE_KEY_TARGET"
    conn.close()


def test_10_unexpected_extra_schema_column(tmp_path):
    """10. Unexpected extra schema column fails safely when allow_extra_columns is false."""
    db_path = str(tmp_path / "test.db")
    csv_file = str(tmp_path / "extra_col.csv")
    with open(csv_file, "w", encoding="utf-8") as f:
        f.write("customer_id,customer_name,email,created_at,extra_unauthorized_col\n")
        f.write("CUST-00001,Alice,alice@example.com,2026-01-01T10:00:00Z,999\n")

    cfg_path = create_test_config(
        tmp_path,
        dataset_name="customers",
        source_file=csv_file,
        source_format="csv",
        target_table="customers",
        load_mode="upsert",
        primary_key=["customer_id"],
        columns={
            "customer_id": {"type": "string", "nullable": false},
            "customer_name": {"type": "string", "nullable": false},
            "email": {"type": "string", "nullable": true},
            "created_at": {"type": "timestamp", "nullable": false},
        },
        allow_extra_columns=False,
    )

    res = backend.run_pipeline(cfg_path, db_path=db_path)
    assert res["status"] == "FAILED"
    assert "Unexpected extra columns" in res["failure_reason"]


def test_11_missing_schema_column(tmp_path):
    """11. Missing schema column fails safely."""
    db_path = str(tmp_path / "test.db")
    csv_file = str(tmp_path / "missing_col.csv")
    with open(csv_file, "w", encoding="utf-8") as f:
        f.write("customer_id,email,created_at\n")
        f.write("CUST-00001,alice@example.com,2026-01-01T10:00:00Z\n")

    cfg_path = create_test_config(
        tmp_path,
        dataset_name="customers",
        source_file=csv_file,
        source_format="csv",
        target_table="customers",
        load_mode="upsert",
        primary_key=["customer_id"],
        columns={
            "customer_id": {"type": "string", "nullable": false},
            "customer_name": {"type": "string", "nullable": false},
            "email": {"type": "string", "nullable": true},
            "created_at": {"type": "timestamp", "nullable": false},
        },
        required_fields=["customer_id", "customer_name", "created_at"],
    )

    res = backend.run_pipeline(cfg_path, db_path=db_path)
    assert res["status"] == "FAILED"
    assert "Missing required columns in CSV header" in res["failure_reason"]


def test_12_wrong_data_type(tmp_path):
    """12. Wrong data type (non-numeric string in integer column)."""
    db_path = str(tmp_path / "test.db")
    csv_file = str(tmp_path / "wrong_type.csv")
    with open(csv_file, "w", encoding="utf-8") as f:
        f.write("order_id,customer_id,product_id,quantity,unit_price,order_timestamp\n")
        f.write("ORD-00001,CUST-00001,PROD-00001,NOT_AN_INT,19.99,2026-01-01T10:00:00Z\n")

    cfg_path = create_test_config(
        tmp_path,
        dataset_name="orders",
        source_file=csv_file,
        source_format="csv",
        target_table="orders",
        load_mode="append",
        primary_key=["order_id"],
        columns={
            "order_id": {"type": "string", "nullable": false},
            "customer_id": {"type": "string", "nullable": false},
            "product_id": {"type": "string", "nullable": false},
            "quantity": {"type": "integer", "nullable": false},
            "unit_price": {"type": "decimal", "nullable": false},
            "order_timestamp": {"type": "timestamp", "nullable": false},
        },
    )

    res = backend.run_pipeline(cfg_path, db_path=db_path)
    assert res["status"] == "PARTIAL_SUCCESS"
    assert res["rejected_count"] == 1

    conn = database.get_connection(db_path)
    rejects = database.get_rejected_records(conn, run_id=res["run_id"])
    assert rejects[0]["reason_code"] == "WRONG_DATA_TYPE"
    conn.close()


def test_13_invalid_customer_foreign_key(tmp_path):
    """13. Invalid customer foreign key."""
    db_path = str(tmp_path / "test.db")
    conn = database.get_connection(db_path)
    database.initialize_schema(conn)
    # Insert valid product
    conn.execute(
        "INSERT INTO products VALUES ('PROD-00001', 'Widget', 10.0, 1, 'src', 'run', 'now');"
    )
    conn.commit()
    conn.close()

    csv_file = str(tmp_path / "bad_cust_fk.csv")
    with open(csv_file, "w", encoding="utf-8") as f:
        f.write("order_id,customer_id,product_id,quantity,unit_price,order_timestamp\n")
        f.write("ORD-00001,CUST-NONEXISTENT,PROD-00001,2,10.00,2026-01-01T10:00:00Z\n")

    cfg_path = create_test_config(
        tmp_path,
        dataset_name="orders",
        source_file=csv_file,
        source_format="csv",
        target_table="orders",
        load_mode="append",
        primary_key=["order_id"],
        columns={
            "order_id": {"type": "string", "nullable": false},
            "customer_id": {"type": "string", "nullable": false},
            "product_id": {"type": "string", "nullable": false},
            "quantity": {"type": "integer", "nullable": false},
            "unit_price": {"type": "decimal", "nullable": false},
            "order_timestamp": {"type": "timestamp", "nullable": false},
        },
        foreign_keys=[
            {"column": "customer_id", "reference_table": "customers", "reference_column": "customer_id"},
            {"column": "product_id", "reference_table": "products", "reference_column": "product_id"},
        ],
        transformations=[
            {"target_column": "total_amount", "type": "multiply_decimal", "operand1": "quantity", "operand2": "unit_price", "decimals": 2}
        ],
    )

    res = backend.run_pipeline(cfg_path, db_path=db_path)
    assert res["status"] == "PARTIAL_SUCCESS"
    assert res["rejected_count"] == 1

    conn = database.get_connection(db_path)
    rejects = database.get_rejected_records(conn, run_id=res["run_id"])
    assert rejects[0]["reason_code"] == "INVALID_CUSTOMER_FK"
    conn.close()


def test_14_invalid_product_foreign_key(tmp_path):
    """14. Invalid product foreign key."""
    db_path = str(tmp_path / "test.db")
    conn = database.get_connection(db_path)
    database.initialize_schema(conn)
    # Insert valid customer
    conn.execute(
        "INSERT INTO customers VALUES ('CUST-00001', 'Alice', 'a@a.com', '2026-01-01T00:00:00Z', 'src', 'run', 'now');"
    )
    conn.commit()
    conn.close()

    csv_file = str(tmp_path / "bad_prod_fk.csv")
    with open(csv_file, "w", encoding="utf-8") as f:
        f.write("order_id,customer_id,product_id,quantity,unit_price,order_timestamp\n")
        f.write("ORD-00001,CUST-00001,PROD-NONEXISTENT,2,10.00,2026-01-01T10:00:00Z\n")

    cfg_path = create_test_config(
        tmp_path,
        dataset_name="orders",
        source_file=csv_file,
        source_format="csv",
        target_table="orders",
        load_mode="append",
        primary_key=["order_id"],
        columns={
            "order_id": {"type": "string", "nullable": false},
            "customer_id": {"type": "string", "nullable": false},
            "product_id": {"type": "string", "nullable": false},
            "quantity": {"type": "integer", "nullable": false},
            "unit_price": {"type": "decimal", "nullable": false},
            "order_timestamp": {"type": "timestamp", "nullable": false},
        },
        foreign_keys=[
            {"column": "customer_id", "reference_table": "customers", "reference_column": "customer_id"},
            {"column": "product_id", "reference_table": "products", "reference_column": "product_id"},
        ],
        transformations=[
            {"target_column": "total_amount", "type": "multiply_decimal", "operand1": "quantity", "operand2": "unit_price", "decimals": 2}
        ],
    )

    res = backend.run_pipeline(cfg_path, db_path=db_path)
    assert res["status"] == "PARTIAL_SUCCESS"
    assert res["rejected_count"] == 1

    conn = database.get_connection(db_path)
    rejects = database.get_rejected_records(conn, run_id=res["run_id"])
    assert rejects[0]["reason_code"] == "INVALID_PRODUCT_FK"
    conn.close()


def test_15_zero_and_negative_quantity(tmp_path):
    """15. Zero and negative quantity domain rejection."""
    db_path = str(tmp_path / "test.db")
    csv_file = str(tmp_path / "bad_qty.csv")
    with open(csv_file, "w", encoding="utf-8") as f:
        f.write("order_id,customer_id,product_id,quantity,unit_price,order_timestamp\n")
        f.write("ORD-00001,CUST-00001,PROD-00001,0,10.00,2026-01-01T10:00:00Z\n")
        f.write("ORD-00002,CUST-00001,PROD-00001,-3,10.00,2026-01-01T10:05:00Z\n")

    cfg_path = create_test_config(
        tmp_path,
        dataset_name="orders",
        source_file=csv_file,
        source_format="csv",
        target_table="orders",
        load_mode="append",
        primary_key=["order_id"],
        columns={
            "order_id": {"type": "string", "nullable": false},
            "customer_id": {"type": "string", "nullable": false},
            "product_id": {"type": "string", "nullable": false},
            "quantity": {"type": "integer", "nullable": false},
            "unit_price": {"type": "decimal", "nullable": false},
            "order_timestamp": {"type": "timestamp", "nullable": false},
        },
        validation_rules=[{"column": "quantity", "rule": "min_value", "min": 1}],
    )

    res = backend.run_pipeline(cfg_path, db_path=db_path)
    assert res["status"] == "PARTIAL_SUCCESS"
    assert res["rejected_count"] == 2

    conn = database.get_connection(db_path)
    rejects = database.get_rejected_records(conn, run_id=res["run_id"])
    assert all(r["reason_code"] == "INVALID_QUANTITY" for r in rejects)
    conn.close()


def test_16_negative_price(tmp_path):
    """16. Negative price rejection."""
    db_path = str(tmp_path / "test.db")
    json_file = str(tmp_path / "neg_price.json")
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump([{"product_id": "PROD-00001", "product_name": "Bad Price", "unit_price": -5.99, "active": True}], f)

    cfg_path = create_test_config(
        tmp_path,
        dataset_name="products",
        source_file=json_file,
        source_format="json",
        target_table="products",
        load_mode="upsert",
        primary_key=["product_id"],
        columns={
            "product_id": {"type": "string", "nullable": false},
            "product_name": {"type": "string", "nullable": false},
            "unit_price": {"type": "decimal", "nullable": false},
            "active": {"type": "boolean", "nullable": false},
        },
        validation_rules=[{"column": "unit_price", "rule": "min_value", "min": "0.00"}],
    )

    res = backend.run_pipeline(cfg_path, db_path=db_path)
    assert res["status"] == "PARTIAL_SUCCESS"
    assert res["rejected_count"] == 1

    conn = database.get_connection(db_path)
    rejects = database.get_rejected_records(conn, run_id=res["run_id"])
    assert rejects[0]["reason_code"] == "NEGATIVE_PRICE"
    conn.close()


def test_17_boundary_monetary_values(tmp_path):
    """17. Boundary monetary values: 0.00 price, 999999.99, and avoiding drift (3 * 0.10 == 0.30)."""
    db_path = str(tmp_path / "test.db")
    conn = database.get_connection(db_path)
    database.initialize_schema(conn)
    conn.execute("INSERT INTO customers VALUES ('CUST-00001', 'Alice', 'a@a.com', '2026-01-01T00:00:00Z', 's', 'r', 'n');")
    conn.execute("INSERT INTO products VALUES ('PROD-00001', 'Free Item', 0.0, 1, 's', 'r', 'n');")
    conn.commit()
    conn.close()

    csv_file = str(tmp_path / "boundary_money.csv")
    with open(csv_file, "w", encoding="utf-8") as f:
        f.write("order_id,customer_id,product_id,quantity,unit_price,order_timestamp\n")
        f.write("ORD-BND-1,CUST-00001,PROD-00001,1,0.00,2026-01-01T10:00:00Z\n")
        f.write("ORD-BND-2,CUST-00001,PROD-00001,3,0.10,2026-01-01T10:01:00Z\n")
        f.write("ORD-BND-3,CUST-00001,PROD-00001,1,999999.99,2026-01-01T10:02:00Z\n")

    cfg_path = create_test_config(
        tmp_path,
        dataset_name="orders",
        source_file=csv_file,
        source_format="csv",
        target_table="orders",
        load_mode="append",
        primary_key=["order_id"],
        columns={
            "order_id": {"type": "string", "nullable": false},
            "customer_id": {"type": "string", "nullable": false},
            "product_id": {"type": "string", "nullable": false},
            "quantity": {"type": "integer", "nullable": false},
            "unit_price": {"type": "decimal", "nullable": false},
            "order_timestamp": {"type": "timestamp", "nullable": false},
        },
        foreign_keys=[
            {"column": "customer_id", "reference_table": "customers", "reference_column": "customer_id"},
            {"column": "product_id", "reference_table": "products", "reference_column": "product_id"},
        ],
        transformations=[
            {"target_column": "total_amount", "type": "multiply_decimal", "operand1": "quantity", "operand2": "unit_price", "decimals": 2}
        ],
        validation_rules=[
            {"column": "quantity", "rule": "min_value", "min": 1},
            {"column": "unit_price", "rule": "min_value", "min": "0.00"},
        ],
    )

    res = backend.run_pipeline(cfg_path, db_path=db_path)
    assert res["status"] == "SUCCESS"
    assert res["accepted_count"] == 3

    conn = database.get_connection(db_path)
    orders = database.get_table_preview(conn, "orders", limit=10)
    # Check 3 * 0.10 = 0.30 exact
    ord_bnd_2 = [o for o in orders if o["order_id"] == "ORD-BND-2"][0]
    assert ord_bnd_2["total_amount"] == 0.30
    conn.close()


def test_18_late_out_of_order_events(tmp_path):
    """18. Late/out-of-order events handled deterministically and recorded in history."""
    db_path = str(tmp_path / "test.db")
    conn = database.get_connection(db_path)
    database.initialize_schema(conn)
    conn.execute("INSERT INTO customers VALUES ('CUST-00001', 'Alice', 'a@a.com', '2026-01-01T00:00:00Z', 's', 'r', 'n');")
    conn.execute("INSERT INTO products VALUES ('PROD-00001', 'Gadget', 20.0, 1, 's', 'r', 'n');")
    conn.commit()
    conn.close()

    # Step 1: Insert newer event
    csv_file1 = str(tmp_path / "order_v1.csv")
    with open(csv_file1, "w", encoding="utf-8") as f:
        f.write("order_id,customer_id,product_id,quantity,unit_price,order_timestamp\n")
        f.write("ORD-00001,CUST-00001,PROD-00001,5,20.00,2026-01-01T15:00:00Z\n")

    cfg_path = create_test_config(
        tmp_path,
        dataset_name="orders",
        source_file=csv_file1,
        source_format="csv",
        target_table="orders",
        load_mode="upsert",
        primary_key=["order_id"],
        columns={
            "order_id": {"type": "string", "nullable": false},
            "customer_id": {"type": "string", "nullable": false},
            "product_id": {"type": "string", "nullable": false},
            "quantity": {"type": "integer", "nullable": false},
            "unit_price": {"type": "decimal", "nullable": false},
            "order_timestamp": {"type": "timestamp", "nullable": false},
        },
        transformations=[
            {"target_column": "total_amount", "type": "multiply_decimal", "operand1": "quantity", "operand2": "unit_price", "decimals": 2}
        ],
    )
    res1 = backend.run_pipeline(cfg_path, db_path=db_path)
    assert res1["inserted_count"] == 1

    # Step 2: Late-arriving older event arrives
    csv_file2 = str(tmp_path / "order_v2_late.csv")
    with open(csv_file2, "w", encoding="utf-8") as f:
        f.write("order_id,customer_id,product_id,quantity,unit_price,order_timestamp\n")
        f.write("ORD-00001,CUST-00001,PROD-00001,2,20.00,2026-01-01T10:00:00Z\n")  # older timestamp

    res2 = backend.run_pipeline(cfg_path, db_path=db_path, override_source_path=csv_file2)
    assert res2["status"] == "SUCCESS"
    assert res2["skipped_count"] == 1  # Target preserved

    conn = database.get_connection(db_path)
    # Target still has quantity 5
    orders = database.get_table_preview(conn, "orders")
    assert orders[0]["quantity"] == 5

    # Change history recorded the late arrival
    hist = database.get_order_change_history(conn)
    assert len(hist) == 1
    assert hist[0]["change_type"] == "LATE_EVENT_RECORDED"
    assert hist[0]["new_quantity"] == 2
    conn.close()


def test_19_identical_input_rerun_idempotency(tmp_path):
    """19. Identical-input rerun idempotency (zero duplicates created, skipped in upsert)."""
    db_path = str(tmp_path / "test.db")
    csv_file = str(tmp_path / "customers.csv")
    with open(csv_file, "w", encoding="utf-8") as f:
        f.write("customer_id,customer_name,email,created_at\n")
        f.write("CUST-00001,Alice Smith,alice@example.com,2026-01-01T10:00:00Z\n")

    cfg_path = create_test_config(
        tmp_path,
        dataset_name="customers",
        source_file=csv_file,
        source_format="csv",
        target_table="customers",
        load_mode="upsert",
        primary_key=["customer_id"],
        columns={
            "customer_id": {"type": "string", "nullable": false},
            "customer_name": {"type": "string", "nullable": false},
            "email": {"type": "string", "nullable": true},
            "created_at": {"type": "timestamp", "nullable": false},
        },
    )

    res1 = backend.run_pipeline(cfg_path, db_path=db_path)
    assert res1["inserted_count"] == 1

    res2 = backend.run_pipeline(cfg_path, db_path=db_path)
    assert res2["inserted_count"] == 0
    assert res2["updated_count"] == 0
    assert res2["skipped_count"] == 1

    conn = database.get_connection(db_path)
    assert database.get_table_count(conn, "customers") == 1
    conn.close()


def test_20_upsert_changed_record(tmp_path):
    """20. Upsert changed record (detects modification and updates row)."""
    db_path = str(tmp_path / "test.db")
    json_file1 = str(tmp_path / "prod1.json")
    with open(json_file1, "w", encoding="utf-8") as f:
        json.dump([{"product_id": "PROD-00001", "product_name": "Switch", "unit_price": 50.0, "active": True}], f)

    cfg_path = create_test_config(
        tmp_path,
        dataset_name="products",
        source_file=json_file1,
        source_format="json",
        target_table="products",
        load_mode="upsert",
        primary_key=["product_id"],
        columns={
            "product_id": {"type": "string", "nullable": false},
            "product_name": {"type": "string", "nullable": false},
            "unit_price": {"type": "decimal", "nullable": false},
            "active": {"type": "boolean", "nullable": false},
        },
    )

    res1 = backend.run_pipeline(cfg_path, db_path=db_path)
    assert res1["inserted_count"] == 1

    # Update price to 65.00
    json_file2 = str(tmp_path / "prod2.json")
    with open(json_file2, "w", encoding="utf-8") as f:
        json.dump([{"product_id": "PROD-00001", "product_name": "Switch", "unit_price": 65.0, "active": True}], f)

    res2 = backend.run_pipeline(cfg_path, db_path=db_path, override_source_path=json_file2)
    assert res2["updated_count"] == 1
    assert res2["inserted_count"] == 0

    conn = database.get_connection(db_path)
    prods = database.get_table_preview(conn, "products")
    assert prods[0]["unit_price"] == 65.0
    conn.close()


def test_21_append_semantics(tmp_path):
    """21. Append semantics (reject duplicate business keys, insert only fresh)."""
    db_path = str(tmp_path / "test.db")
    conn = database.get_connection(db_path)
    database.initialize_schema(conn)
    conn.execute("INSERT INTO customers VALUES ('CUST-00001', 'Alice', 'a@a.com', '2026-01-01T00:00:00Z', 's', 'r', 'n');")
    conn.execute("INSERT INTO products VALUES ('PROD-00001', 'Gadget', 20.0, 1, 's', 'r', 'n');")
    conn.commit()
    conn.close()

    csv_file1 = str(tmp_path / "orders1.csv")
    with open(csv_file1, "w", encoding="utf-8") as f:
        f.write("order_id,customer_id,product_id,quantity,unit_price,order_timestamp\n")
        f.write("ORD-00001,CUST-00001,PROD-00001,1,10.00,2026-01-01T10:00:00Z\n")

    cfg_path = create_test_config(
        tmp_path,
        dataset_name="orders",
        source_file=csv_file1,
        source_format="csv",
        target_table="orders",
        load_mode="append",
        primary_key=["order_id"],
        columns={
            "order_id": {"type": "string", "nullable": false},
            "customer_id": {"type": "string", "nullable": false},
            "product_id": {"type": "string", "nullable": false},
            "quantity": {"type": "integer", "nullable": false},
            "unit_price": {"type": "decimal", "nullable": false},
            "order_timestamp": {"type": "timestamp", "nullable": false},
        },
        transformations=[
            {"target_column": "total_amount", "type": "multiply_decimal", "operand1": "quantity", "operand2": "unit_price", "decimals": 2}
        ],
    )

    res1 = backend.run_pipeline(cfg_path, db_path=db_path)
    assert res1["inserted_count"] == 1

    # Batch 2 contains ORD-00001 (duplicate) and ORD-00002 (new)
    csv_file2 = str(tmp_path / "orders2.csv")
    with open(csv_file2, "w", encoding="utf-8") as f:
        f.write("order_id,customer_id,product_id,quantity,unit_price,order_timestamp\n")
        f.write("ORD-00001,CUST-00001,PROD-00001,1,10.00,2026-01-01T10:00:00Z\n")
        f.write("ORD-00002,CUST-00001,PROD-00001,2,10.00,2026-01-01T10:05:00Z\n")

    res2 = backend.run_pipeline(cfg_path, db_path=db_path, override_source_path=csv_file2)
    assert res2["status"] == "PARTIAL_SUCCESS"
    assert res2["inserted_count"] == 1
    assert res2["rejected_count"] == 1

    conn = database.get_connection(db_path)
    assert database.get_table_count(conn, "orders") == 2
    conn.close()


def test_22_atomic_replace(tmp_path):
    """22. Atomic replace mode (old table replaced cleanly, preserves table on failure)."""
    db_path = str(tmp_path / "test.db")
    csv_file1 = str(tmp_path / "cust_initial.csv")
    with open(csv_file1, "w", encoding="utf-8") as f:
        f.write("customer_id,customer_name,email,created_at\n")
        f.write("CUST-00001,Initial Cust 1,c1@ex.com,2026-01-01T10:00:00Z\n")
        f.write("CUST-00002,Initial Cust 2,c2@ex.com,2026-01-01T10:05:00Z\n")

    cfg_path = create_test_config(
        tmp_path,
        dataset_name="customers",
        source_file=csv_file1,
        source_format="csv",
        target_table="customers",
        load_mode="replace",
        primary_key=["customer_id"],
        columns={
            "customer_id": {"type": "string", "nullable": false},
            "customer_name": {"type": "string", "nullable": false},
            "email": {"type": "string", "nullable": true},
            "created_at": {"type": "timestamp", "nullable": false},
        },
    )

    res1 = backend.run_pipeline(cfg_path, db_path=db_path)
    assert res1["inserted_count"] == 2

    # Replace with single new customer CUST-99999
    csv_file2 = str(tmp_path / "cust_replace.csv")
    with open(csv_file2, "w", encoding="utf-8") as f:
        f.write("customer_id,customer_name,email,created_at\n")
        f.write("CUST-99999,Replacement Cust,rep@ex.com,2026-01-01T12:00:00Z\n")

    res2 = backend.run_pipeline(cfg_path, db_path=db_path, override_source_path=csv_file2)
    assert res2["status"] == "SUCCESS"
    assert res2["inserted_count"] == 1

    conn = database.get_connection(db_path)
    rows = database.get_table_preview(conn, "customers")
    assert len(rows) == 1
    assert rows[0]["customer_id"] == "CUST-99999"

    # Now attempt a failing replace (e.g. malformed CSV missing required column)
    csv_bad = str(tmp_path / "cust_broken.csv")
    with open(csv_bad, "w", encoding="utf-8") as f:
        f.write("customer_id,created_at\n")  # Missing non-nullable customer_name column!
        f.write("CUST-BROKEN,2026-01-01T10:00:00Z\n")

    res3 = backend.run_pipeline(cfg_path, db_path=db_path, override_source_path=csv_bad)
    assert res3["status"] == "FAILED"

    # Verify previous good target table is still intact!
    rows_after = database.get_table_preview(conn, "customers")
    assert len(rows_after) == 1
    assert rows_after[0]["customer_id"] == "CUST-99999"
    conn.close()


def test_23_injected_partial_database_failure_and_retry(tmp_path):
    """23. Injected partial database failure rolls back and retry succeeds."""
    db_path = str(tmp_path / "test.db")
    csv_file = str(tmp_path / "customers.csv")
    with open(csv_file, "w", encoding="utf-8") as f:
        f.write("customer_id,customer_name,email,created_at\n")
        f.write("CUST-00001,Alice Smith,alice@example.com,2026-01-01T10:00:00Z\n")

    cfg_path = create_test_config(
        tmp_path,
        dataset_name="customers",
        source_file=csv_file,
        source_format="csv",
        target_table="customers",
        load_mode="upsert",
        primary_key=["customer_id"],
        columns={
            "customer_id": {"type": "string", "nullable": false},
            "customer_name": {"type": "string", "nullable": false},
            "email": {"type": "string", "nullable": true},
            "created_at": {"type": "timestamp", "nullable": false},
        },
    )

    # Injected DB failure
    res_fail = backend.run_pipeline(cfg_path, db_path=db_path, simulate_db_failure=True)
    assert res_fail["status"] == "FAILED"

    # Verify target table has 0 rows (rolled back)
    conn = database.get_connection(db_path)
    assert database.get_table_count(conn, "customers") == 0

    # Verify audit log recorded FAILED status durably
    run_log = database.get_pipeline_run(conn, res_fail["run_id"])
    assert run_log["status"] == "FAILED"
    conn.close()

    # Retry without simulation
    res_retry = backend.run_pipeline(cfg_path, db_path=db_path, simulate_db_failure=False)
    assert res_retry["status"] == "SUCCESS"
    assert res_retry["inserted_count"] == 1

    conn = database.get_connection(db_path)
    assert database.get_table_count(conn, "customers") == 1
    conn.close()


def test_24_reconciliation_invariants(tmp_path):
    """24. Reconciliation invariants: input = accepted + rejected; accepted = inserted + updated + skipped."""
    db_path = str(tmp_path / "test.db")
    csv_file = str(tmp_path / "reconcile.csv")
    with open(csv_file, "w", encoding="utf-8") as f:
        f.write("customer_id,customer_name,email,created_at\n")
        f.write("CUST-00001,Valid 1,c1@ex.com,2026-01-01T10:00:00Z\n")
        f.write("CUST-00002,,c2@ex.com,2026-01-01T10:05:00Z\n")  # Rejected: missing required name
        f.write("CUST-00003,Valid 3,c3@ex.com,2026-01-01T10:10:00Z\n")

    cfg_path = create_test_config(
        tmp_path,
        dataset_name="customers",
        source_file=csv_file,
        source_format="csv",
        target_table="customers",
        load_mode="upsert",
        primary_key=["customer_id"],
        columns={
            "customer_id": {"type": "string", "nullable": false},
            "customer_name": {"type": "string", "nullable": false},
            "email": {"type": "string", "nullable": true},
            "created_at": {"type": "timestamp", "nullable": false},
        },
        required_fields=["customer_id", "customer_name", "created_at"],
    )

    res = backend.run_pipeline(cfg_path, db_path=db_path)
    assert res["input_count"] == res["accepted_count"] + res["rejected_count"]
    assert res["accepted_count"] == res["inserted_count"] + res["updated_count"] + res["skipped_count"]

    conn = database.get_connection(db_path)
    summary = database.get_reconciliation_summary(conn)
    assert len(summary) >= 1
    assert all(s["all_reconciled"] for s in summary)
    conn.close()


def test_25_audit_log_correctness(tmp_path):
    """25. Audit log correctness: fields accurately populated in pipeline_runs."""
    db_path = str(tmp_path / "test.db")
    csv_file = str(tmp_path / "cust.csv")
    with open(csv_file, "w", encoding="utf-8") as f:
        f.write("customer_id,customer_name,email,created_at\n")
        f.write("CUST-00001,Alice,alice@example.com,2026-01-01T10:00:00Z\n")

    cfg_path = create_test_config(
        tmp_path,
        dataset_name="customers",
        source_file=csv_file,
        source_format="csv",
        target_table="customers",
        load_mode="upsert",
        primary_key=["customer_id"],
        columns={
            "customer_id": {"type": "string", "nullable": false},
            "customer_name": {"type": "string", "nullable": false},
            "email": {"type": "string", "nullable": true},
            "created_at": {"type": "timestamp", "nullable": false},
        },
    )

    res = backend.run_pipeline(cfg_path, db_path=db_path)
    conn = database.get_connection(db_path)
    run_log = database.get_pipeline_run(conn, res["run_id"])
    conn.close()

    assert run_log["run_id"] == res["run_id"]
    assert run_log["dataset"] == "customers"
    assert run_log["status"] == "SUCCESS"
    assert run_log["input_count"] == 1
    assert run_log["accepted_count"] == 1
    assert run_log["rejected_count"] == 0
    assert run_log["elapsed_seconds"] >= 0.0
    assert run_log["started_at"] is not None
    assert run_log["ended_at"] is not None


def test_26_quarantine_traceability(tmp_path):
    """26. Quarantine traceability: rejected_records contains complete debug metadata."""
    db_path = str(tmp_path / "test.db")
    csv_file = str(tmp_path / "bad_data.csv")
    with open(csv_file, "w", encoding="utf-8") as f:
        f.write("customer_id,customer_name,email,created_at\n")
        f.write(",No PK User,user@example.com,2026-01-01T10:00:00Z\n")

    cfg_path = create_test_config(
        tmp_path,
        dataset_name="customers",
        source_file=csv_file,
        source_format="csv",
        target_table="customers",
        load_mode="upsert",
        primary_key=["customer_id"],
        columns={
            "customer_id": {"type": "string", "nullable": false},
            "customer_name": {"type": "string", "nullable": false},
            "email": {"type": "string", "nullable": true},
            "created_at": {"type": "timestamp", "nullable": false},
        },
    )

    res = backend.run_pipeline(cfg_path, db_path=db_path)
    conn = database.get_connection(db_path)
    rejects = database.get_rejected_records(conn, run_id=res["run_id"])
    conn.close()

    assert len(rejects) == 1
    rej = rejects[0]
    assert rej["run_id"] == res["run_id"]
    assert rej["dataset"] == "customers"
    assert rej["source_file"] == csv_file
    assert rej["row_number"] == 2
    assert "No PK User" in rej["raw_payload"]
    assert rej["reason_code"] == "NULL_PRIMARY_KEY"
    assert "null or empty" in rej["reason_detail"]
    assert rej["rejected_at"] is not None


def test_27_config_validation_and_unsafe_identifiers(tmp_path):
    """27. Config validation and unsafe identifiers rejected before execution."""
    # Attempt SQL Injection via table name
    with pytest.raises(Exception):
        database.validate_identifier("customers; DROP TABLE users;--", database.ALLOWED_TABLES)

    # Attempt config with invalid identifier
    bad_cfg = {
        "dataset_name": "customers; DROP TABLE customers;--",
        "source": {"path": "a.csv", "format": "csv"},
        "target": {"table": "customers", "load_mode": "upsert"},
        "primary_key": ["id"],
        "columns": {"id": {"type": "string"}},
    }
    cfg_path = str(tmp_path / "injection_config.json")
    with open(cfg_path, "w", encoding="utf-8") as f:
        json.dump(bad_cfg, f)

    with pytest.raises(ValueError):
        backend.load_and_validate_config(cfg_path)


def test_28_large_data_generation_determinism(tmp_path):
    """28. Large data generation determinism: identical seed produces identical datasets."""
    dir1 = str(tmp_path / "gen1")
    dir2 = str(tmp_path / "gen2")

    cust1 = generate_data.generate_customers(100)
    cust2 = generate_data.generate_customers(100)
    assert cust1 == cust2

    prod1 = generate_data.generate_products(50)
    prod2 = generate_data.generate_products(50)
    assert prod1 == prod2

    ord1 = generate_data.generate_orders(200, [c["customer_id"] for c in cust1], prod1)
    ord2 = generate_data.generate_orders(200, [c["customer_id"] for c in cust2], prod2)
    assert ord1 == ord2
