"""
scripts/generate_data.py - Deterministic Data Generator for ConfigFlow ETL Platform.

Generates:
1. Deterministic starter datasets (customers CSV, products JSON, orders CSV) with fixed seed (42).
2. Large configurable benchmark datasets (e.g. 100,000+ orders).
3. Labeled invalid cases in a separate fixture manifest (tests/fixtures/invalid_manifest.json).
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
import json
import os
import random
import sys
from typing import Any, Dict, List

BASE_DATETIME = datetime(2026, 1, 1, 8, 0, 0, tzinfo=timezone.utc)

FIRST_NAMES = [
    "Alex", "Jordan", "Taylor", "Morgan", "Sam", "Chris", "Pat", "Riley",
    "Casey", "Jamie", "Avery", "Cameron", "Dakota", "Reese", "Quinn"
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson"
]
PRODUCT_CATEGORIES = [
    "Edge Gateway", "Sensor Node", "Compute Module", "Optical Transceiver",
    "Power Supply", "Industrial Switch", "Memory Card", "Processor Board"
]


def generate_customers(count: int) -> List[Dict[str, Any]]:
    """Generate deterministic customer records."""
    customers = []
    for i in range(1, count + 1):
        fn = FIRST_NAMES[(i * 3) % len(FIRST_NAMES)]
        ln = LAST_NAMES[(i * 7) % len(LAST_NAMES)]
        name = f"{fn} {ln}"
        email = f"{fn.lower()}.{ln.lower()}{i}@example.com" if (i % 10 != 0) else None
        created_at = (BASE_DATETIME + timedelta(minutes=i * 15)).isoformat()
        customers.append(
            {
                "customer_id": f"CUST-{i:05d}",
                "customer_name": name,
                "email": email,
                "created_at": created_at,
            }
        )
    return customers


def generate_products(count: int) -> List[Dict[str, Any]]:
    """Generate deterministic product records."""
    products = []
    for i in range(1, count + 1):
        cat = PRODUCT_CATEGORIES[(i * 5) % len(PRODUCT_CATEGORIES)]
        name = f"Yotta-{cat} V{i % 5 + 1}"
        # Price range from $4.99 to $899.99
        base_price = (i * 17.35) % 850 + 4.99
        unit_price = float(Decimal(str(base_price)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
        active = bool(i % 15 != 0)
        products.append(
            {
                "product_id": f"PROD-{i:05d}",
                "product_name": name,
                "unit_price": unit_price,
                "active": active,
            }
        )
    return products


def generate_orders(
    count: int,
    customer_ids: List[str],
    products: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Generate deterministic order records."""
    orders = []
    prod_map = {p["product_id"]: p["unit_price"] for p in products}
    prod_keys = list(prod_map.keys())

    num_cust = len(customer_ids)
    num_prod = len(prod_keys)

    for i in range(1, count + 1):
        cust_id = customer_ids[i % num_cust]
        prod_id = prod_keys[(i * 7) % num_prod]
        unit_price = prod_map[prod_id]
        quantity = (i % 9) + 1  # 1 to 9
        order_ts = (BASE_DATETIME + timedelta(seconds=i * 30)).isoformat()

        orders.append(
            {
                "order_id": f"ORD-{i:07d}",
                "customer_id": cust_id,
                "product_id": prod_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "order_timestamp": order_ts,
            }
        )
    return orders


def write_customers_csv(customers: List[Dict[str, Any]], filepath: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
    with open(filepath, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["customer_id", "customer_name", "email", "created_at"])
        for c in customers:
            writer.writerow([c["customer_id"], c["customer_name"], c["email"] or "", c["created_at"]])


def write_products_json(products: List[Dict[str, Any]], filepath: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(products, f, indent=2)


def write_orders_csv(orders: List[Dict[str, Any]], filepath: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
    with open(filepath, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "customer_id", "product_id", "quantity", "unit_price", "order_timestamp"])
        for o in orders:
            writer.writerow([
                o["order_id"],
                o["customer_id"],
                o["product_id"],
                o["quantity"],
                f"{o['unit_price']:.2f}",
                o["order_timestamp"],
            ])


def generate_invalid_fixture_manifest(fixtures_dir: str) -> str:
    """
    Generate labeled invalid test cases in a separate fixture manifest and files.
    """
    os.makedirs(fixtures_dir, exist_ok=True)

    manifest = {
        "description": "ConfigFlow ETL Invalid & Boundary Fixtures Manifest",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "fixtures": {
            "empty_csv": {
                "file": "empty.csv",
                "format": "csv",
                "expected_status": "SUCCESS",
                "expected_input_count": 0,
                "expected_rejected_count": 0,
                "description": "Completely empty 0-byte CSV file",
            },
            "empty_json": {
                "file": "empty.json",
                "format": "json",
                "expected_status": "SUCCESS",
                "expected_input_count": 0,
                "expected_rejected_count": 0,
                "description": "Empty JSON array []",
            },
            "malformed_csv_row": {
                "file": "malformed_row.csv",
                "format": "csv",
                "expected_status": "PARTIAL_SUCCESS",
                "expected_rejected_code": "MALFORMED_ROW",
                "description": "Row with uneven comma/column count",
            },
            "malformed_json_syntax": {
                "file": "malformed_syntax.json",
                "format": "json",
                "expected_status": "FAILED",
                "expected_failure_substring": "Malformed JSON",
                "description": "Invalid JSON syntax causing batch parse failure",
            },
            "missing_required_field": {
                "file": "missing_required.csv",
                "format": "csv",
                "expected_rejected_code": "MISSING_REQUIRED_FIELD",
                "description": "Record missing non-nullable customer_name",
            },
            "null_primary_key": {
                "file": "null_pk.csv",
                "format": "csv",
                "expected_rejected_code": "NULL_PRIMARY_KEY",
                "description": "Record with empty primary key string",
            },
            "duplicate_source_key": {
                "file": "duplicate_key.csv",
                "format": "csv",
                "expected_rejected_code": "DUPLICATE_KEY_IN_BATCH",
                "description": "Two rows sharing identical primary key in same batch",
            },
            "unexpected_extra_column": {
                "file": "extra_column.csv",
                "format": "csv",
                "expected_status": "FAILED",
                "expected_failure_substring": "Unexpected extra columns",
                "description": "CSV with unauthorized extra column discount_pct",
            },
            "missing_schema_column": {
                "file": "missing_col.csv",
                "format": "csv",
                "expected_status": "FAILED",
                "expected_failure_substring": "Missing required columns in CSV header",
                "description": "CSV header missing required column customer_name",
            },
            "wrong_data_type": {
                "file": "wrong_type.csv",
                "format": "csv",
                "expected_rejected_code": "WRONG_DATA_TYPE",
                "description": "Quantity containing string 'NOT_AN_INT'",
            },
            "invalid_customer_fk": {
                "file": "invalid_cust_fk.csv",
                "format": "csv",
                "expected_rejected_code": "INVALID_CUSTOMER_FK",
                "description": "Order referencing non-existent customer CUST-99999",
            },
            "invalid_product_fk": {
                "file": "invalid_prod_fk.csv",
                "format": "csv",
                "expected_rejected_code": "INVALID_PRODUCT_FK",
                "description": "Order referencing non-existent product PROD-99999",
            },
            "zero_negative_quantity": {
                "file": "zero_neg_qty.csv",
                "format": "csv",
                "expected_rejected_code": "INVALID_QUANTITY",
                "description": "Orders with quantity 0 and -5",
            },
            "negative_price": {
                "file": "negative_price.csv",
                "format": "csv",
                "expected_rejected_code": "NEGATIVE_PRICE",
                "description": "Order with negative price -12.50",
            },
            "boundary_monetary_values": {
                "file": "boundary_money.csv",
                "format": "csv",
                "expected_status": "SUCCESS",
                "description": "Orders with 0.00 price, 999999.99 price, and drift-prone 0.10 * 3",
            },
        },
    }

    # 1. empty.csv
    with open(os.path.join(fixtures_dir, "empty.csv"), "w", encoding="utf-8") as f:
        pass

    # 2. empty.json
    with open(os.path.join(fixtures_dir, "empty.json"), "w", encoding="utf-8") as f:
        f.write("[]")

    # 3. malformed_row.csv
    with open(os.path.join(fixtures_dir, "malformed_row.csv"), "w", encoding="utf-8") as f:
        f.write("customer_id,customer_name,email,created_at\n")
        f.write("CUST-00001,Valid Name,valid@example.com,2026-01-01T10:00:00Z\n")
        f.write("CUST-00002,Too,Few,Columns\n")  # Bad row

    # 4. malformed_syntax.json
    with open(os.path.join(fixtures_dir, "malformed_syntax.json"), "w", encoding="utf-8") as f:
        f.write("[{'product_id': 'PROD-1', broken json }")

    # 5. missing_required.csv
    with open(os.path.join(fixtures_dir, "missing_required.csv"), "w", encoding="utf-8") as f:
        f.write("customer_id,customer_name,email,created_at\n")
        f.write("CUST-00001,,user@example.com,2026-01-01T10:00:00Z\n")  # Missing name

    # 6. null_pk.csv
    with open(os.path.join(fixtures_dir, "null_pk.csv"), "w", encoding="utf-8") as f:
        f.write("customer_id,customer_name,email,created_at\n")
        f.write(",John Doe,user@example.com,2026-01-01T10:00:00Z\n")  # Missing PK

    # 7. duplicate_key.csv
    with open(os.path.join(fixtures_dir, "duplicate_key.csv"), "w", encoding="utf-8") as f:
        f.write("customer_id,customer_name,email,created_at\n")
        f.write("CUST-00001,Original Name,user1@example.com,2026-01-01T10:00:00Z\n")
        f.write("CUST-00001,Duplicate Name,user2@example.com,2026-01-01T11:00:00Z\n")

    # 8. extra_column.csv
    with open(os.path.join(fixtures_dir, "extra_column.csv"), "w", encoding="utf-8") as f:
        f.write("customer_id,customer_name,email,created_at,discount_pct\n")
        f.write("CUST-00001,John Doe,user@example.com,2026-01-01T10:00:00Z,15\n")

    # 9. missing_col.csv
    with open(os.path.join(fixtures_dir, "missing_col.csv"), "w", encoding="utf-8") as f:
        f.write("customer_id,email,created_at\n")
        f.write("CUST-00001,user@example.com,2026-01-01T10:00:00Z\n")

    # 10. wrong_type.csv
    with open(os.path.join(fixtures_dir, "wrong_type.csv"), "w", encoding="utf-8") as f:
        f.write("order_id,customer_id,product_id,quantity,unit_price,order_timestamp\n")
        f.write("ORD-00001,CUST-00001,PROD-00001,NOT_AN_INT,19.99,2026-01-01T10:00:00Z\n")

    # 11. invalid_cust_fk.csv
    with open(os.path.join(fixtures_dir, "invalid_cust_fk.csv"), "w", encoding="utf-8") as f:
        f.write("order_id,customer_id,product_id,quantity,unit_price,order_timestamp\n")
        f.write("ORD-00001,CUST-99999,PROD-00001,2,19.99,2026-01-01T10:00:00Z\n")

    # 12. invalid_prod_fk.csv
    with open(os.path.join(fixtures_dir, "invalid_prod_fk.csv"), "w", encoding="utf-8") as f:
        f.write("order_id,customer_id,product_id,quantity,unit_price,order_timestamp\n")
        f.write("ORD-00001,CUST-00001,PROD-99999,2,19.99,2026-01-01T10:00:00Z\n")

    # 13. zero_neg_qty.csv
    with open(os.path.join(fixtures_dir, "zero_neg_qty.csv"), "w", encoding="utf-8") as f:
        f.write("order_id,customer_id,product_id,quantity,unit_price,order_timestamp\n")
        f.write("ORD-00001,CUST-00001,PROD-00001,0,19.99,2026-01-01T10:00:00Z\n")
        f.write("ORD-00002,CUST-00001,PROD-00001,-5,19.99,2026-01-01T10:05:00Z\n")

    # 14. negative_price.csv
    with open(os.path.join(fixtures_dir, "negative_price.csv"), "w", encoding="utf-8") as f:
        f.write("order_id,customer_id,product_id,quantity,unit_price,order_timestamp\n")
        f.write("ORD-00001,CUST-00001,PROD-00001,2,-12.50,2026-01-01T10:00:00Z\n")

    # 15. boundary_money.csv
    with open(os.path.join(fixtures_dir, "boundary_money.csv"), "w", encoding="utf-8") as f:
        f.write("order_id,customer_id,product_id,quantity,unit_price,order_timestamp\n")
        f.write("ORD-BND-1,CUST-00001,PROD-00001,1,0.00,2026-01-01T10:00:00Z\n")
        f.write("ORD-BND-2,CUST-00001,PROD-00001,3,0.10,2026-01-01T10:01:00Z\n")
        f.write("ORD-BND-3,CUST-00001,PROD-00001,1,999999.99,2026-01-01T10:02:00Z\n")

    manifest_path = os.path.join(fixtures_dir, "invalid_manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    return manifest_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="ConfigFlow ETL Platform - Deterministic Data Generator",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--customers", type=int, default=50, help="Number of customer records to generate.")
    parser.add_argument("--products", type=int, default=20, help="Number of product records to generate.")
    parser.add_argument("--orders", type=int, default=100, help="Number of order records to generate.")
    parser.add_argument("--seed", type=int, default=42, help="Deterministic random seed.")
    parser.add_argument("--out-dir", type=str, default="data/starter", help="Output directory for generated datasets.")
    parser.add_argument("--generate-fixtures", action="store_true", default=True, help="Generate invalid test fixtures manifest.")

    args = parser.parse_args()

    random.seed(args.seed)
    print(f"[*] Initializing deterministic generator (seed={args.seed})...")

    # Generate starter data
    customers = generate_customers(args.customers)
    products = generate_products(args.products)
    orders = generate_orders(args.orders, [c["customer_id"] for c in customers], products)

    cust_path = os.path.join(args.out_dir, "customers.csv")
    prod_path = os.path.join(args.out_dir, "products.json")
    ord_path = os.path.join(args.out_dir, "orders.csv")

    write_customers_csv(customers, cust_path)
    write_products_json(products, prod_path)
    write_orders_csv(orders, ord_path)

    print(f"[+] Wrote {len(customers)} customers to {cust_path}")
    print(f"[+] Wrote {len(products)} products to {prod_path}")
    print(f"[+] Wrote {len(orders)} orders to {ord_path}")

    if args.generate_fixtures:
        manifest_path = generate_invalid_fixture_manifest("tests/fixtures")
        print(f"[+] Generated invalid test fixtures manifest at {manifest_path}")

    print("[*] Deterministic generation complete.")


if __name__ == "__main__":
    main()
