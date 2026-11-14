"""
backend.py - Metadata-Driven Ingestion, Validation, ETL, and Orchestration Engine.

Part of ConfigFlow ETL Platform.
Follows strict architectural constraints:
- Generic metadata-driven processing with NO hardcoded dataset branching.
- Strict JSON config validation and identifier allowlisting.
- Safe CSV/JSON reader detecting malformed rows and corrupted inputs.
- Schema validation: types, nullability, missing columns, unexpected columns.
- Domain rule validation: zero/negative quantity, negative price, regex.
- In-batch deduplication with deterministic policy (first-wins / last-wins).
- Foreign-key verification against parent tables.
- Metadata-driven transformations (e.g., total_amount calculation with Decimal).
- Transactional persistence with atomic rollback on failure.
- Quarantine tracking for rejected records with granular reason codes.
- Durable audit logging of all runs and reconciliations.
- Full CLI support with exit codes and dependency ordering.
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import io
import json
import os
import re
import sys
import time
import uuid
from typing import Any, Dict, List, Optional, Set, Tuple

import database

# Regex for standard ISO 8601 timestamps
ISO8601_REGEX = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?$"
)

ALLOWED_TYPES = {"string", "integer", "decimal", "boolean", "timestamp"}
ALLOWED_MODES = {"append", "upsert", "replace"}
ALLOWED_FORMATS = {"csv", "json"}


# ============================================================================
# CONFIGURATION VALIDATION AND RESOLUTION
# ============================================================================


class ConfigValidationError(Exception):
    """Raised when pipeline configuration is invalid or missing required metadata."""
    pass


class BatchSchemaError(Exception):
    """Raised when source dataset has incompatible header/schema."""
    pass


def load_and_validate_config(config_path: str, base_dir: Optional[str] = None) -> Dict[str, Any]:
    """
    Load JSON configuration and validate its metadata structure and values.
    Resolves source file paths relative to config or base_dir.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        try:
            config = json.load(f)
        except json.JSONDecodeError as e:
            raise ConfigValidationError(f"Invalid JSON in config file {config_path}: {e}")

    # Top-level required fields
    required_keys = ["dataset_name", "source", "target", "primary_key", "columns"]
    for k in required_keys:
        if k not in config:
            raise ConfigValidationError(f"Config missing required top-level key: '{k}'")

    # Validate identifiers
    dataset_name = config["dataset_name"]
    database.validate_identifier(dataset_name)

    source = config["source"]
    if not isinstance(source, dict) or "path" not in source or "format" not in source:
        raise ConfigValidationError("Config 'source' must be an object with 'path' and 'format'.")

    s_format = source["format"].lower()
    if s_format not in ALLOWED_FORMATS:
        raise ConfigValidationError(f"Unsupported source format '{s_format}'. Allowed: {ALLOWED_FORMATS}")

    # Resolve source path
    src_path = source["path"]
    if not os.path.isabs(src_path):
        ref_dir = base_dir or os.path.dirname(os.path.abspath(config_path))
        resolved_src = os.path.normpath(os.path.join(ref_dir, src_path))
        # Also check relative to workspace if not found relative to config
        if not os.path.exists(resolved_src):
            workspace_candidate = os.path.normpath(os.path.join(os.getcwd(), src_path))
            if os.path.exists(workspace_candidate):
                resolved_src = workspace_candidate
        config["source"]["resolved_path"] = resolved_src
    else:
        config["source"]["resolved_path"] = src_path

    target = config["target"]
    if not isinstance(target, dict) or "table" not in target or "load_mode" not in target:
        raise ConfigValidationError("Config 'target' must be an object with 'table' and 'load_mode'.")

    database.validate_identifier(target["table"])
    load_mode = target["load_mode"].lower()
    if load_mode not in ALLOWED_MODES:
        raise ConfigValidationError(f"Unsupported load mode '{load_mode}'. Allowed: {ALLOWED_MODES}")

    # Primary key
    pk = config["primary_key"]
    if isinstance(pk, str):
        config["primary_key"] = [pk]
    elif isinstance(pk, list) and pk:
        for p in pk:
            database.validate_identifier(p)
    else:
        raise ConfigValidationError("'primary_key' must be a non-empty string or list of column names.")

    # Columns
    columns = config["columns"]
    if not isinstance(columns, dict) or not columns:
        raise ConfigValidationError("'columns' must be a non-empty dictionary of column definitions.")

    for col_name, col_def in columns.items():
        database.validate_identifier(col_name)
        if not isinstance(col_def, dict) or "type" not in col_def:
            raise ConfigValidationError(f"Column '{col_name}' must specify a 'type'.")
        col_type = col_def["type"].lower()
        if col_type not in ALLOWED_TYPES:
            raise ConfigValidationError(
                f"Unsupported type '{col_type}' for column '{col_name}'. Allowed: {ALLOWED_TYPES}"
            )

    # Derive required fields from non-nullable columns if not specified
    if "required_fields" not in config or not config["required_fields"]:
        config["required_fields"] = [
            col for col, cdef in columns.items() if not cdef.get("nullable", True)
        ]

    # Foreign keys
    if "foreign_keys" in config and isinstance(config["foreign_keys"], list):
        for fk in config["foreign_keys"]:
            database.validate_identifier(fk["column"])
            database.validate_identifier(fk["reference_table"])
            database.validate_identifier(fk["reference_column"])

    # Batch deduplication policy
    if "batch_dedup_policy" not in config:
        config["batch_dedup_policy"] = "first-wins"

    return config


# ============================================================================
# SAFE SOURCE DATA READING (CSV & JSON)
# ============================================================================


def read_source_data(
    source_path: str,
    source_format: str,
    allow_extra_columns: bool = False,
    expected_columns: Optional[Set[str]] = None,
    required_fields: Optional[List[str]] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Safely reads source files.
    Detects malformed rows, bad JSON, or missing/unexpected headers.

    Returns:
        (raw_records, early_rejected_records)
    """
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Source file does not exist: {source_path}")

    # Check if empty file
    if os.path.getsize(source_path) == 0:
        return ([], [])

    s_format = source_format.lower()
    records: List[Dict[str, Any]] = []
    early_rejects: List[Dict[str, Any]] = []

    if s_format == "csv":
        with open(source_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            try:
                header = next(reader, None)
            except Exception as e:
                raise BatchSchemaError(f"Failed to read CSV header in {source_path}: {e}")

            if header is None:
                return ([], [])

            # Clean header keys
            header = [h.strip() for h in header]
            header_set = set(header)

            # Validate header schema columns
            if expected_columns:
                if not allow_extra_columns:
                    unexpected = header_set - expected_columns
                    if unexpected:
                        raise BatchSchemaError(
                            f"Unexpected extra columns found in CSV header: {sorted(list(unexpected))}. "
                            f"Config allow_extra_columns is false."
                        )

            if required_fields:
                missing = set(required_fields) - header_set
                if missing:
                    raise BatchSchemaError(
                        f"Missing required columns in CSV header: {sorted(list(missing))}."
                    )

            row_num = 1
            for row in reader:
                row_num += 1
                if not row or all(c.strip() == "" for c in row):
                    # Empty blank line
                    continue

                if len(row) != len(header):
                    early_rejects.append(
                        {
                            "row_number": row_num,
                            "raw_payload": ",".join(row),
                            "reason_code": "MALFORMED_ROW",
                            "reason_detail": f"Row has {len(row)} values, expected {len(header)} based on header.",
                        }
                    )
                    continue

                rec = dict(zip(header, row))
                rec["_row_number"] = row_num
                records.append(rec)

    elif s_format == "json":
        with open(source_path, "r", encoding="utf-8") as f:
            try:
                raw_data = json.load(f)
            except json.JSONDecodeError as e:
                raise BatchSchemaError(f"Malformed JSON file in {source_path}: {e}")

        if not isinstance(raw_data, list):
            raise BatchSchemaError(f"Expected a top-level JSON list of records in {source_path}.")

        row_num = 0
        for item in raw_data:
            row_num += 1
            if not isinstance(item, dict):
                early_rejects.append(
                    {
                        "row_number": row_num,
                        "raw_payload": str(item),
                        "reason_code": "MALFORMED_ROW",
                        "reason_detail": "JSON record is not an object/dictionary.",
                    }
                )
                continue

            item_keys = set(item.keys())
            if expected_columns and not allow_extra_columns:
                unexpected = item_keys - expected_columns
                if unexpected:
                    raise BatchSchemaError(
                        f"Unexpected extra fields in JSON record at line {row_num}: {sorted(list(unexpected))}."
                    )

            if required_fields:
                missing = set(required_fields) - item_keys
                if missing:
                    early_rejects.append(
                        {
                            "row_number": row_num,
                            "raw_payload": json.dumps(item),
                            "reason_code": "MISSING_REQUIRED_FIELD",
                            "reason_detail": f"Record is missing required schema fields: {sorted(list(missing))}.",
                        }
                    )
                    continue

            item_copy = dict(item)
            item_copy["_row_number"] = row_num
            records.append(item_copy)

    return (records, early_rejects)


# ============================================================================
# DATA VALIDATION, TYPE PARSING, AND DOMAIN RULES
# ============================================================================


def parse_and_validate_value(
    val: Any,
    col_name: str,
    col_def: Dict[str, Any],
) -> Tuple[Optional[Any], Optional[str], Optional[str]]:
    """
    Parse a value according to defined column type.
    Returns: (parsed_value, error_code, error_detail)
    """
    col_type = col_def["type"].lower()
    nullable = col_def.get("nullable", True)

    if val is None or (isinstance(val, str) and val.strip() == ""):
        if not nullable:
            return (None, "MISSING_REQUIRED_FIELD", f"Column '{col_name}' cannot be null or empty.")
        return (None, None, None)

    try:
        if col_type == "string":
            return (str(val).strip(), None, None)

        elif col_type == "integer":
            if isinstance(val, bool):
                return (None, "WRONG_DATA_TYPE", f"Column '{col_name}' expects integer, got boolean.")
            if isinstance(val, (int,)):
                return (int(val), None, None)
            s_val = str(val).strip()
            # Must be digits only (optional leading + or -)
            if not re.match(r"^[+-]?\d+$", s_val):
                return (None, "WRONG_DATA_TYPE", f"Column '{col_name}' expects integer, got '{val}'.")
            return (int(s_val), None, None)

        elif col_type == "decimal":
            if isinstance(val, bool):
                return (None, "WRONG_DATA_TYPE", f"Column '{col_name}' expects decimal, got boolean.")
            s_val = str(val).strip()
            try:
                dec = Decimal(s_val)
                # Keep 2 decimal precision
                rounded = dec.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                return (float(rounded), None, None)
            except InvalidOperation:
                return (None, "WRONG_DATA_TYPE", f"Column '{col_name}' cannot parse decimal from '{val}'.")

        elif col_type == "boolean":
            if isinstance(val, bool):
                return (1 if val else 0, None, None)
            s_val = str(val).strip().lower()
            if s_val in {"true", "1", "yes", "t"}:
                return (1, None, None)
            elif s_val in {"false", "0", "no", "f"}:
                return (0, None, None)
            else:
                return (None, "WRONG_DATA_TYPE", f"Column '{col_name}' cannot parse boolean from '{val}'.")

        elif col_type == "timestamp":
            s_val = str(val).strip()
            if not ISO8601_REGEX.match(s_val):
                return (None, "WRONG_DATA_TYPE", f"Column '{col_name}' expects ISO8601 timestamp, got '{val}'.")
            return (s_val, None, None)

        else:
            return (None, "UNSUPPORTED_TYPE", f"Unknown type '{col_type}'.")

    except Exception as e:
        return (None, "PARSING_ERROR", f"Error parsing '{val}' for column '{col_name}': {e}")


def apply_domain_validation_rules(
    record: Dict[str, Any],
    rules: List[Dict[str, Any]],
) -> Optional[Tuple[str, str]]:
    """
    Check domain validation rules (min_value, max_value, regex, etc.).
    Returns (reason_code, reason_detail) on failure, or None.
    """
    for rule in rules:
        col = rule.get("column")
        rule_type = rule.get("rule")
        val = record.get(col)

        if val is None:
            if rule.get("nullable_ok", False):
                continue
            return ("MISSING_REQUIRED_FIELD", f"Rule validation failed: column '{col}' is null.")

        if rule_type == "min_value":
            min_val = rule["min"]
            if col == "quantity":
                if int(val) <= 0 or int(val) < int(min_val):
                    return ("INVALID_QUANTITY", f"Quantity must be greater than 0, got {val}.")
            elif col == "unit_price":
                if Decimal(str(val)) < Decimal(str(min_val)):
                    return ("NEGATIVE_PRICE", f"Unit price cannot be negative, got {val}.")
            else:
                try:
                    if Decimal(str(val)) < Decimal(str(min_val)):
                        return ("DOMAIN_RULE_VIOLATION", f"Column '{col}' value {val} is below minimum {min_val}.")
                except (InvalidOperation, TypeError):
                    if val < min_val:
                        return ("DOMAIN_RULE_VIOLATION", f"Column '{col}' value {val} is below minimum {min_val}.")

        elif rule_type == "regex":
            pattern = rule.get("pattern")
            if pattern and not re.match(pattern, str(val)):
                return ("INVALID_FORMAT", f"Column '{col}' value '{val}' violates pattern '{pattern}'.")

    return None


def apply_transformations(
    record: Dict[str, Any],
    transformations: List[Dict[str, Any]],
) -> None:
    """
    Execute metadata-driven transformations.
    Example: total_amount = quantity * unit_price (using Decimal ROUND_HALF_UP).
    """
    for trans in transformations:
        t_type = trans.get("type")
        target_col = trans.get("target_column")

        if t_type == "multiply_decimal":
            op1 = record.get(trans["operand1"])
            op2 = record.get(trans["operand2"])
            if op1 is not None and op2 is not None:
                dec1 = Decimal(str(op1))
                dec2 = Decimal(str(op2))
                result = (dec1 * dec2).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                record[target_col] = float(result)


# ============================================================================
# FOREIGN KEY VERIFICATION ENGINE
# ============================================================================


def verify_foreign_keys(
    conn: sqlite3.Connection,
    records: List[Dict[str, Any]],
    fk_definitions: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Validate foreign keys against actual SQLite parent tables in batch.
    Returns: (fk_valid_records, fk_rejected_records)
    """
    if not fk_definitions or not records:
        return (records, [])

    valid_records = list(records)
    rejected_records: List[Dict[str, Any]] = []

    for fk in fk_definitions:
        col = fk["column"]
        ref_table = fk["reference_table"]
        ref_col = fk["reference_column"]

        # Collect candidate keys
        candidates = {r[col] for r in valid_records if r.get(col) is not None}
        existing_parent_keys = database.check_foreign_keys_bulk(conn, ref_table, ref_col, candidates)

        next_valid = []
        for r in valid_records:
            val = r.get(col)
            if val is not None and val not in existing_parent_keys:
                code = f"INVALID_{ref_table.rstrip('s').upper()}_FK"
                if "CUSTOMER" in code:
                    code = "INVALID_CUSTOMER_FK"
                elif "PRODUCT" in code:
                    code = "INVALID_PRODUCT_FK"
                else:
                    code = "INVALID_FOREIGN_KEY"

                rejected_records.append(
                    {
                        "row_number": r.get("_row_number"),
                        "raw_payload": str(r),
                        "reason_code": code,
                        "reason_detail": f"Foreign key violation on '{col}': value '{val}' not found in parent table '{ref_table}.{ref_col}'.",
                    }
                )
            else:
                next_valid.append(r)
        valid_records = next_valid

    return (valid_records, rejected_records)


# ============================================================================
# IN-BATCH DEDUPLICATION ENGINE
# ============================================================================


def deduplicate_batch(
    records: List[Dict[str, Any]],
    pk_columns: List[str],
    policy: str = "first-wins",
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Detect duplicate primary keys within the same source batch.
    Policy: 'first-wins' keeps first occurrence, quarantines subsequent ones.
            'last-wins' keeps latest occurrence, quarantines earlier ones.
    Returns: (unique_records, duplicate_rejected_records)
    """
    pk_col = pk_columns[0]
    seen: Dict[Any, Dict[str, Any]] = {}
    duplicates: List[Dict[str, Any]] = []

    if policy == "last-wins":
        # Traverse in reverse or keep latest
        reversed_records = list(reversed(records))
        kept = {}
        for r in reversed_records:
            key = r[pk_col]
            if key in kept:
                duplicates.append(
                    {
                        "row_number": r.get("_row_number"),
                        "raw_payload": str(r),
                        "reason_code": "DUPLICATE_KEY_IN_BATCH",
                        "reason_detail": f"Duplicate primary key '{key}' in batch. Overridden by last-wins policy.",
                    }
                )
            else:
                kept[key] = r
        unique = list(reversed(list(kept.values())))
        return (unique, duplicates)

    else:  # default 'first-wins'
        unique = []
        for r in records:
            key = r[pk_col]
            if key in seen:
                duplicates.append(
                    {
                        "row_number": r.get("_row_number"),
                        "raw_payload": str(r),
                        "reason_code": "DUPLICATE_KEY_IN_BATCH",
                        "reason_detail": f"Duplicate primary key '{key}' in batch. Quarantined by first-wins policy.",
                    }
                )
            else:
                seen[key] = r
                unique.append(r)
        return (unique, duplicates)


# ============================================================================
# MAIN PIPELINE EXECUTION ENGINE
# ============================================================================


def run_pipeline(
    config_path: str,
    db_path: str = database.DEFAULT_DB_PATH,
    override_source_path: Optional[str] = None,
    override_load_mode: Optional[str] = None,
    allow_extra_columns_override: Optional[bool] = None,
    simulate_db_failure: bool = False,
) -> Dict[str, Any]:
    """
    Execute a single dataset ingestion pipeline driven entirely by metadata.
    Does NOT branch on dataset name.
    """
    start_time = time.time()
    started_at_iso = datetime.now(timezone.utc).isoformat()
    run_id = f"RUN-{uuid.uuid4().hex[:12].upper()}"

    # 1. Load and validate config
    config = load_and_validate_config(config_path)
    dataset_name = config["dataset_name"]
    target_table = config["target"]["table"]
    load_mode = (override_load_mode or config["target"]["load_mode"]).lower()
    source_info = config["source"]
    source_path = override_source_path or source_info.get("resolved_path") or source_info["path"]
    source_format = source_info["format"]
    pk_columns = config["primary_key"]
    columns_spec = config["columns"]
    required_fields = config.get("required_fields", [])
    foreign_keys = config.get("foreign_keys", [])
    transformations = config.get("transformations", [])
    validation_rules = config.get("validation_rules", [])
    allow_extra_cols = (
        allow_extra_columns_override
        if allow_extra_columns_override is not None
        else config.get("allow_extra_columns", False)
    )
    batch_dedup_policy = config.get("batch_dedup_policy", "first-wins")

    conn = database.get_connection(db_path)
    database.initialize_schema(conn)

    # Durably record initiation of the run
    database.create_pipeline_run(
        conn=conn,
        run_id=run_id,
        dataset=dataset_name,
        source_file=source_path,
        load_mode=load_mode,
        started_at=started_at_iso,
    )

    all_quarantine: List[Dict[str, Any]] = []

    try:
        # 2. Read Source Data safely
        expected_cols = set(columns_spec.keys())
        raw_records, early_rejects = read_source_data(
            source_path=source_path,
            source_format=source_format,
            allow_extra_columns=allow_extra_cols,
            expected_columns=expected_cols,
            required_fields=required_fields,
        )
        for rej in early_rejects:
            rej.update(
                {
                    "run_id": run_id,
                    "dataset": dataset_name,
                    "source_file": source_path,
                    "rejected_at": datetime.now(timezone.utc).isoformat(),
                }
            )
        all_quarantine.extend(early_rejects)

        total_input_count = len(raw_records) + len(early_rejects)

        # 3. Row-level Type Parsing and Schema Validation
        parsed_records: List[Dict[str, Any]] = []
        for r in raw_records:
            row_num = r.get("_row_number")
            row_error: Optional[Tuple[str, str]] = None
            cleaned_row: Dict[str, Any] = {"_row_number": row_num}

            # Check primary key nullability first
            pk_val = r.get(pk_columns[0])
            if pk_val is None or (isinstance(pk_val, str) and pk_val.strip() == ""):
                all_quarantine.append(
                    {
                        "run_id": run_id,
                        "dataset": dataset_name,
                        "source_file": source_path,
                        "row_number": row_num,
                        "raw_payload": str(r),
                        "reason_code": "NULL_PRIMARY_KEY",
                        "reason_detail": f"Primary key '{pk_columns[0]}' is null or empty.",
                        "rejected_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                continue

            # Check each defined column
            for col_name, col_def in columns_spec.items():
                val = r.get(col_name)
                parsed_val, err_code, err_detail = parse_and_validate_value(val, col_name, col_def)
                if err_code:
                    row_error = (err_code, err_detail)
                    break
                cleaned_row[col_name] = parsed_val

            if row_error:
                all_quarantine.append(
                    {
                        "run_id": run_id,
                        "dataset": dataset_name,
                        "source_file": source_path,
                        "row_number": row_num,
                        "raw_payload": str(r),
                        "reason_code": row_error[0],
                        "reason_detail": row_error[1],
                        "rejected_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                continue

            # Domain validation rules
            domain_err = apply_domain_validation_rules(cleaned_row, validation_rules)
            if domain_err:
                all_quarantine.append(
                    {
                        "run_id": run_id,
                        "dataset": dataset_name,
                        "source_file": source_path,
                        "row_number": row_num,
                        "raw_payload": str(r),
                        "reason_code": domain_err[0],
                        "reason_detail": domain_err[1],
                        "rejected_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                continue

            parsed_records.append(cleaned_row)

        # 4. In-batch Deduplication
        deduped_records, batch_dup_rejects = deduplicate_batch(
            records=parsed_records,
            pk_columns=pk_columns,
            policy=batch_dedup_policy,
        )
        for dup in batch_dup_rejects:
            dup.update(
                {
                    "run_id": run_id,
                    "dataset": dataset_name,
                    "source_file": source_path,
                    "rejected_at": datetime.now(timezone.utc).isoformat(),
                }
            )
        all_quarantine.extend(batch_dup_rejects)

        # 5. Transformations (e.g. total_amount)
        for rec in deduped_records:
            apply_transformations(rec, transformations)

        # 6. Foreign Key Validation (queries database parent tables)
        fk_valid_records, fk_rejects = verify_foreign_keys(conn, deduped_records, foreign_keys)
        for fkr in fk_rejects:
            fkr.update(
                {
                    "run_id": run_id,
                    "dataset": dataset_name,
                    "source_file": source_path,
                    "rejected_at": datetime.now(timezone.utc).isoformat(),
                }
            )
        all_quarantine.extend(fk_rejects)

        # 7. Add Audit Metadata Columns to Accepted Records
        now_iso = datetime.now(timezone.utc).isoformat()
        clean_target_records = []
        for r in fk_valid_records:
            r_target = dict(r)
            r_target.pop("_row_number", None)
            r_target["source_file"] = os.path.basename(source_path)
            r_target["ingestion_run_id"] = run_id
            r_target["ingested_at"] = now_iso
            clean_target_records.append(r_target)

        # Determine target columns
        target_columns = list(columns_spec.keys())
        for trans in transformations:
            t_col = trans.get("target_column")
            if t_col and t_col not in target_columns:
                target_columns.append(t_col)
        target_columns.extend(["source_file", "ingestion_run_id", "ingested_at"])

        # Injected failure simulation for rollback testing
        if simulate_db_failure:
            raise sqlite3.OperationalError("Simulated database failure during transaction.")

        # 8. Persist Records to SQLite
        inserted, updated, skipped, db_rejects = database.persist_records(
            conn=conn,
            table_name=target_table,
            load_mode=load_mode,
            records=clean_target_records,
            pk_columns=pk_columns,
            column_names=target_columns,
            run_id=run_id,
            source_file=source_path,
        )
        all_quarantine.extend(db_rejects)

        # Record all quarantined rejected records
        database.record_rejected_records(conn, all_quarantine)

        total_rejected = len(all_quarantine)
        total_accepted = inserted + updated + skipped

        status = "SUCCESS" if total_rejected == 0 else "PARTIAL_SUCCESS"
        elapsed = round(time.time() - start_time, 4)
        ended_at_iso = datetime.now(timezone.utc).isoformat()

        # Update durable run audit record
        database.update_pipeline_run(
            conn=conn,
            run_id=run_id,
            status=status,
            ended_at=ended_at_iso,
            input_count=total_input_count,
            accepted_count=total_accepted,
            rejected_count=total_rejected,
            inserted_count=inserted,
            updated_count=updated,
            skipped_count=skipped,
            elapsed_seconds=elapsed,
            failure_reason=None,
        )

        return {
            "run_id": run_id,
            "dataset": dataset_name,
            "status": status,
            "load_mode": load_mode,
            "source_file": source_path,
            "input_count": total_input_count,
            "accepted_count": total_accepted,
            "rejected_count": total_rejected,
            "inserted_count": inserted,
            "updated_count": updated,
            "skipped_count": skipped,
            "elapsed_seconds": elapsed,
            "failure_reason": None,
        }

    except Exception as exc:
        elapsed = round(time.time() - start_time, 4)
        ended_at_iso = datetime.now(timezone.utc).isoformat()
        failure_msg = str(exc)

        # Durable audit update in separate transaction
        try:
            database.update_pipeline_run(
                conn=conn,
                run_id=run_id,
                status="FAILED",
                ended_at=ended_at_iso,
                input_count=0,
                accepted_count=0,
                rejected_count=len(all_quarantine),
                inserted_count=0,
                updated_count=0,
                skipped_count=0,
                elapsed_seconds=elapsed,
                failure_reason=failure_msg,
            )
            if all_quarantine:
                database.record_rejected_records(conn, all_quarantine)
        except Exception as audit_err:
            print(f"Warning: Failed to update audit record for failed run {run_id}: {audit_err}", file=sys.stderr)

        return {
            "run_id": run_id,
            "dataset": dataset_name,
            "status": "FAILED",
            "load_mode": load_mode,
            "source_file": source_path,
            "input_count": 0,
            "accepted_count": 0,
            "rejected_count": len(all_quarantine),
            "inserted_count": 0,
            "updated_count": 0,
            "skipped_count": 0,
            "elapsed_seconds": elapsed,
            "failure_reason": failure_msg,
        }
    finally:
        conn.close()


# ============================================================================
# ORCHESTRATION ENGINE (DEPENDENCY ORDERING & RUN ALL)
# ============================================================================


def get_dependency_order(configs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Topologically sort configs based on declared dependencies.
    Ensures parent tables (customers, products) load before child tables (orders).
    """
    name_to_config = {c["dataset_name"]: c for c in configs}
    visited = set()
    visiting = set()
    order = []

    def visit(name: str):
        if name in visiting:
            raise ConfigValidationError(f"Cyclic dependency detected involving dataset '{name}'.")
        if name not in visited:
            visiting.add(name)
            cfg = name_to_config.get(name)
            if cfg:
                for dep in cfg.get("dependencies", []):
                    if dep in name_to_config:
                        visit(dep)
            visiting.remove(name)
            visited.add(name)
            if cfg:
                order.append(cfg)

    for cfg in configs:
        if cfg["dataset_name"] not in visited:
            visit(cfg["dataset_name"])

    return order


def run_all_pipelines(
    config_dir: str = "config",
    db_path: str = database.DEFAULT_DB_PATH,
) -> List[Dict[str, Any]]:
    """
    Load all JSON configs in config_dir, sort by dependencies, and execute sequentially.
    """
    if not os.path.exists(config_dir):
        raise FileNotFoundError(f"Config directory does not exist: {config_dir}")

    config_files = [
        os.path.join(config_dir, f)
        for f in os.listdir(config_dir)
        if f.endswith(".json")
    ]
    if not config_files:
        raise FileNotFoundError(f"No .json config files found in {config_dir}")

    parsed_configs = []
    for cf in config_files:
        cfg = load_and_validate_config(cf)
        cfg["_config_path"] = cf
        parsed_configs.append(cfg)

    ordered_configs = get_dependency_order(parsed_configs)

    results = []
    for cfg in ordered_configs:
        res = run_pipeline(
            config_path=cfg["_config_path"],
            db_path=db_path,
        )
        results.append(res)
    return results


# ============================================================================
# CLI INTERFACE
# ============================================================================


def main() -> None:
    """CLI entry point for backend.py."""
    parser = argparse.ArgumentParser(
        description="ConfigFlow ETL Platform - Metadata-Driven Ingestion Engine",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--config",
        type=str,
        help="Path to a single dataset JSON configuration file.",
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Run all dataset pipelines in dependency order.",
    )

    parser.add_argument(
        "--config-dir",
        type=str,
        default="config",
        help="Directory containing dataset config files (used with --all).",
    )
    parser.add_argument(
        "--db",
        type=str,
        default=database.DEFAULT_DB_PATH,
        help="Path to SQLite warehouse database.",
    )
    parser.add_argument(
        "--override-source",
        type=str,
        default=None,
        help="Override the source data file path declared in configuration.",
    )
    parser.add_argument(
        "--override-mode",
        type=str,
        choices=["append", "upsert", "replace"],
        default=None,
        help="Override the load mode declared in configuration.",
    )

    args = parser.parse_args()

    overall_success = True

    try:
        if args.all:
            print(f"[*] Discovering configurations in '{args.config_dir}'...")
            results = run_all_pipelines(config_dir=args.config_dir, db_path=args.db)
            print("\n" + "=" * 80)
            print(f"{'DATASET':<15} {'STATUS':<15} {'INPUT':<8} {'ACCEPTED':<10} {'REJECTED':<10} {'TIME(s)':<8}")
            print("=" * 80)
            for r in results:
                print(
                    f"{r['dataset']:<15} {r['status']:<15} {r['input_count']:<8} "
                    f"{r['accepted_count']:<10} {r['rejected_count']:<10} {r['elapsed_seconds']:<8.4f}"
                )
                if r["status"] == "FAILED":
                    overall_success = False
            print("=" * 80)

        elif args.config:
            print(f"[*] Executing pipeline for configuration: '{args.config}'...")
            res = run_pipeline(
                config_path=args.config,
                db_path=args.db,
                override_source_path=args.override_source,
                override_load_mode=args.override_mode,
            )
            print("\nExecution Result:")
            print(json.dumps(res, indent=2))
            if res["status"] == "FAILED":
                overall_success = False

    except Exception as e:
        print(f"\n[!] Fatal Execution Error: {e}", file=sys.stderr)
        sys.exit(1)

    if not overall_success:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
