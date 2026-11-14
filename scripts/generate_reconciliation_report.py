"""
scripts/generate_reconciliation_report.py - Generate Truthful Reconciliation Report.

Part of ConfigFlow ETL Platform.
Reads database audit tables and writes evidence/reconciliation.txt.
"""

from __future__ import annotations

import os
import sys

# Ensure root directory is in sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone
import database


def main():
    db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "warehouse.db")
    evidence_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "evidence")
    os.makedirs(evidence_dir, exist_ok=True)

    conn = database.get_connection(db_path)
    summary = database.get_reconciliation_summary(conn)
    rejects = database.get_rejected_records(conn, limit=50)

    lines = []
    lines.append("=" * 80)
    lines.append("CONFIGFLOW ETL PLATFORM - RECONCILIATION & AUDIT VERIFICATION REPORT")
    lines.append("=" * 80)
    lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"Target Warehouse: {db_path}")
    lines.append("")
    lines.append("RECONCILIATION LEDGER BY PIPELINE RUN:")
    lines.append(
        f"{'RUN ID':<18} {'DATASET':<12} {'MODE':<8} {'STATUS':<16} "
        f"{'INPUT':<6} {'ACC':<6} {'REJ':<6} {'INS':<6} {'UPD':<6} {'SKP':<6} {'RECONCILED':<10}"
    )
    lines.append("-" * 105)

    for s in summary:
        lines.append(
            f"{s['run_id']:<18} {s['dataset']:<12} {s['load_mode']:<8} {s['status']:<16} "
            f"{s['input_count']:<6} {s['accepted_count']:<6} {s['rejected_count']:<6} "
            f"{s['inserted_count']:<6} {s['updated_count']:<6} {s['skipped_count']:<6} {str(s['all_reconciled']):<10}"
        )

    lines.append("")
    lines.append("MATHEMATICAL INVARIANTS STATUS:")
    all_inv1 = all(s["inv1_input_reconciled"] for s in summary)
    all_inv2 = all(s["inv2_accepted_reconciled"] for s in summary)
    lines.append(f"1. Input Conservation (input = accepted + rejected): {'PASSED' if all_inv1 else 'FAILED'}")
    lines.append(f"2. Accepted Granularity (accepted = inserted + updated + skipped): {'PASSED' if all_inv2 else 'FAILED'}")
    lines.append("")
    lines.append(f"QUARANTINED REJECTS AUDIT TRAIL ({len(rejects)} records sampled):")
    for r in rejects[:15]:
        lines.append(
            f"- Run: {r['run_id']} | Table: {r['dataset']} | Row: {r['row_number']} | "
            f"Code: {r['reason_code']} | Detail: {r['reason_detail']}"
        )

    lines.append("=" * 80)

    report_text = "\n".join(lines)
    print(report_text)

    report_file = os.path.join(evidence_dir, "reconciliation.txt")
    with open(report_file, "w", encoding="utf-8") as f:
        f.write(report_text)
    print(f"\n[+] Saved reconciliation report to {report_file}")
    conn.close()


if __name__ == "__main__":
    main()
