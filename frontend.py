"""
frontend.py - Professional Streamlit Dashboard for ConfigFlow ETL Platform.

Part of ConfigFlow ETL Platform.
Follows architectural constraints:
- Calls backend and database APIs exclusively; contains NO ETL logic.
- Displays project overview, execution controls, metrics cards, audit logs,
  quarantine viewer, target table previews, reconciliation invariants,
  order change history, and performance benchmarks.
"""

from __future__ import annotations

import json
import os
import sqlite3
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

import backend
import database

# Streamlit Page Configuration
st.set_page_config(
    page_title="ConfigFlow ETL Platform",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for polished enterprise UI
st.markdown(
    """
    <style>
    .metric-card {
        background-color: #f8f9fa;
        border: 1px solid #e9ecef;
        border-radius: 8px;
        padding: 14px;
        text-align: center;
    }
    .badge-success {
        color: #155724;
        background-color: #d4edda;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: 600;
    }
    .badge-partial {
        color: #856404;
        background-color: #fff3cd;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: 600;
    }
    .badge-failed {
        color: #721c24;
        background-color: #f8d7da;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: 600;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def get_db_path() -> str:
    """Return database path from session or default."""
    return st.session_state.get("db_path", database.DEFAULT_DB_PATH)


def load_available_configs() -> Dict[str, str]:
    """Scan config directory for valid configuration files."""
    config_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config")
    configs = {}
    if os.path.exists(config_dir):
        for f in sorted(os.listdir(config_dir)):
            if f.endswith(".json"):
                path = os.path.join(config_dir, f)
                try:
                    with open(path, "r", encoding="utf-8") as jf:
                        data = json.load(jf)
                        d_name = data.get("dataset_name", f[:-5])
                        configs[d_name] = path
                except Exception:
                    configs[f[:-5]] = path
    return configs


# ============================================================================
# SIDEBAR CONTROLS
# ============================================================================

st.sidebar.image(
    "https://img.icons8.com/fluency/96/data-configuration.png",
    width=64,
)
st.sidebar.title("ConfigFlow ETL")
st.sidebar.caption("Metadata-Driven Ingestion Engine")

available_configs = load_available_configs()
config_names = list(available_configs.keys())

st.sidebar.header("Pipeline Control")
selected_dataset = st.sidebar.selectbox("Select Dataset", config_names, index=0 if config_names else None)

override_mode = st.sidebar.selectbox(
    "Override Load Mode",
    ["[Default from Config]", "append", "upsert", "replace"],
    index=0,
)
mode_arg = None if override_mode.startswith("[Default") else override_mode

st.sidebar.subheader("Execution Actions")
run_selected_btn = st.sidebar.button("🚀 Run Selected Dataset", use_container_width=True, type="primary")
run_all_btn = st.sidebar.button("⚡ Run All in Dependency Order", use_container_width=True)

st.sidebar.divider()
st.sidebar.caption(f"Warehouse DB: `{os.path.basename(get_db_path())}`")
if st.sidebar.button("🔄 Refresh Data", use_container_width=True):
    st.rerun()

# ============================================================================
# PIPELINE EXECUTION TRIGGERS
# ============================================================================

if run_selected_btn and selected_dataset:
    cfg_file = available_configs[selected_dataset]
    with st.spinner(f"Ingesting '{selected_dataset}'..."):
        res = backend.run_pipeline(
            config_path=cfg_file,
            db_path=get_db_path(),
            override_load_mode=mode_arg,
        )
        st.session_state["latest_run"] = res
        if res["status"] == "SUCCESS":
            st.sidebar.success(f"Successfully ingested {res['dataset']} ({res['accepted_count']} rows accepted)")
        elif res["status"] == "PARTIAL_SUCCESS":
            st.sidebar.warning(f"Partial success: {res['accepted_count']} accepted, {res['rejected_count']} rejected")
        else:
            st.sidebar.error(f"Pipeline failed: {res['failure_reason']}")

if run_all_btn:
    with st.spinner("Executing all pipelines in topological dependency order..."):
        all_results = backend.run_all_pipelines(
            config_dir=os.path.join(os.path.dirname(os.path.abspath(__file__)), "config"),
            db_path=get_db_path(),
        )
        st.session_state["latest_batch_results"] = all_results
        if all_results:
            st.session_state["latest_run"] = all_results[-1]
        st.sidebar.success(f"Executed {len(all_results)} dataset pipelines in dependency order.")


# ============================================================================
# MAIN DASHBOARD INTERFACE
# ============================================================================

st.title("🔄 ConfigFlow ETL – Metadata-Driven Ingestion Platform")
st.markdown(
    "Metadata-driven ingestion engine featuring schema validation, foreign-key enforcement, "
    "domain data-quality checks, transactional persistence (append, upsert, atomic replace), and quarantine traceability."
)

conn = database.get_connection(get_db_path())
database.initialize_schema(conn)

tabs = st.tabs([
    "📊 Pipeline Operations",
    "⚖️ Reconciliation & Audit",
    "🛡️ Quarantine & Quality",
    "📜 Order Change History",
    "🔍 Target Tables",
    "⚡ Performance Benchmarks",
])

# ----------------------------------------------------------------------------
# TAB 1: PIPELINE OPERATIONS & METRICS
# ----------------------------------------------------------------------------
with tabs[0]:
    runs = database.get_pipeline_runs(conn, limit=1)
    latest_run = st.session_state.get("latest_run") or (runs[0] if runs else None)

    if latest_run:
        status = latest_run.get("status", "UNKNOWN")
        badge_class = (
            "badge-success" if status == "SUCCESS"
            else "badge-partial" if status == "PARTIAL_SUCCESS"
            else "badge-failed"
        )

        st.subheader("Latest Pipeline Run Overview")
        col_hdr1, col_hdr2, col_hdr3, col_hdr4 = st.columns(4)
        with col_hdr1:
            st.markdown(f"**Run ID:** `{latest_run.get('run_id')}`")
        with col_hdr2:
            st.markdown(f"**Dataset:** `{latest_run.get('dataset')}`")
        with col_hdr3:
            st.markdown(f"**Status:** <span class='{badge_class}'>{status}</span>", unsafe_allow_html=True)
        with col_hdr4:
            st.markdown(f"**Load Mode:** `{latest_run.get('load_mode', 'N/A')}`")

        if latest_run.get("failure_reason"):
            st.error(f"Failure Reason: {latest_run.get('failure_reason')}")

        st.markdown("### Operational Metrics")
        m_c1, m_c2, m_c3, m_c4, m_c5, m_c6, m_c7 = st.columns(7)
        m_c1.metric("Input Rows", latest_run.get("input_count", 0))
        m_c2.metric("Accepted Rows", latest_run.get("accepted_count", 0))
        m_c3.metric("Inserted", latest_run.get("inserted_count", 0))
        m_c4.metric("Updated", latest_run.get("updated_count", 0))
        m_c5.metric("Skipped", latest_run.get("skipped_count", 0))
        m_c6.metric("Rejected", latest_run.get("rejected_count", 0))
        m_c7.metric("Runtime (s)", f"{latest_run.get('elapsed_seconds', 0.0):.4f}")

    else:
        st.info("No pipeline runs recorded yet. Use the sidebar buttons to launch an ingestion run.")

    st.divider()
    st.subheader("Target Warehouse Overview")
    t1, t2, t3, t4, t5 = st.columns(5)
    t1.metric("Customers in Warehouse", database.get_table_count(conn, "customers"))
    t2.metric("Products in Warehouse", database.get_table_count(conn, "products"))
    t3.metric("Orders in Warehouse", database.get_table_count(conn, "orders"))
    t4.metric("Total Quarantined Rejects", database.get_table_count(conn, "rejected_records"))
    t5.metric("Order Change Audits", database.get_table_count(conn, "order_change_history"))


# ----------------------------------------------------------------------------
# TAB 2: RECONCILIATION & AUDIT LEDGER
# ----------------------------------------------------------------------------
with tabs[1]:
    st.subheader("⚖️ Pipeline Reconciliation & Mathematical Invariants")
    st.markdown(
        """
        **Mathematical Invariants Enforced:**
        1. **Input Conservation:** `input_count = accepted_count + rejected_count`
        2. **Accepted Granularity:** `accepted_count = inserted_count + updated_count + skipped_count`
        """
    )
    rec_summary = database.get_reconciliation_summary(conn)
    if rec_summary:
        df_rec = pd.DataFrame(rec_summary)
        st.dataframe(
            df_rec[[
                "run_id", "dataset", "status", "load_mode", "input_count",
                "accepted_count", "rejected_count", "inserted_count",
                "updated_count", "skipped_count", "all_reconciled"
            ]],
            use_container_width=True,
            hide_index=True,
        )
        all_passed = all(r["all_reconciled"] for r in rec_summary)
        if all_passed:
            st.success("✅ All pipeline runs strictly satisfy reconciliation conservation invariants.")
        else:
            st.error("❌ One or more runs violated reconciliation invariants!")
    else:
        st.info("No runs available for reconciliation.")

    st.divider()
    st.subheader("📜 Historical Pipeline Runs Ledger")
    historical_runs = database.get_pipeline_runs(conn, limit=50)
    if historical_runs:
        st.dataframe(pd.DataFrame(historical_runs), use_container_width=True, hide_index=True)
    else:
        st.info("No historical runs found.")


# ----------------------------------------------------------------------------
# TAB 3: QUARANTINE & QUALITY
# ----------------------------------------------------------------------------
with tabs[2]:
    st.subheader("🛡️ Quarantined Rejected Records")
    st.markdown(
        "Invalid records are safely quarantined with full lineage: run ID, source file, line number, "
        "raw JSON payload, machine-readable reason code, and descriptive reason detail."
    )

    all_rejects = database.get_rejected_records(conn, limit=200)
    if all_rejects:
        df_rej = pd.DataFrame(all_rejects)

        col_q1, col_q2 = st.columns([1, 2])
        with col_q1:
            st.markdown("#### Reason Code Distribution")
            code_counts = df_rej["reason_code"].value_counts().reset_index()
            code_counts.columns = ["Reason Code", "Count"]
            st.dataframe(code_counts, use_container_width=True, hide_index=True)

        with col_q2:
            st.markdown("#### Filter Quarantined Records")
            selected_code = st.selectbox(
                "Filter by Reason Code",
                ["All"] + sorted(df_rej["reason_code"].unique().tolist()),
            )

        filtered_rej = df_rej if selected_code == "All" else df_rej[df_rej["reason_code"] == selected_code]

        st.dataframe(
            filtered_rej[[
                "reject_id", "run_id", "dataset", "row_number",
                "reason_code", "reason_detail", "raw_payload", "rejected_at"
            ]],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.success("🎉 No records currently in quarantine. All ingested rows were valid!")


# ----------------------------------------------------------------------------
# TAB 4: ORDER CHANGE HISTORY
# ----------------------------------------------------------------------------
with tabs[3]:
    st.subheader("📜 Order Change History & Late-Arrival Audit")
    st.markdown(
        "Append-only change log recording every price change, quantity modification, and "
        "late-arriving out-of-order event for transactional orders."
    )
    change_history = database.get_order_change_history(conn, limit=100)
    if change_history:
        st.dataframe(pd.DataFrame(change_history), use_container_width=True, hide_index=True)
    else:
        st.info("No order changes or late-arriving events recorded yet.")


# ----------------------------------------------------------------------------
# TAB 5: TARGET TABLES PREVIEW
# ----------------------------------------------------------------------------
with tabs[4]:
    st.subheader("🔍 Target Warehouse Table Previews")
    tbl_choice = st.selectbox("Select Target Table", ["customers", "products", "orders"])
    preview_limit = st.slider("Preview Rows", 10, 100, 25)

    preview_rows = database.get_table_preview(conn, tbl_choice, limit=preview_limit)
    if preview_rows:
        st.dataframe(pd.DataFrame(preview_rows), use_container_width=True, hide_index=True)
    else:
        st.info(f"Target table '{tbl_choice}' is currently empty.")


# ----------------------------------------------------------------------------
# TAB 6: PERFORMANCE BENCHMARKS
# ----------------------------------------------------------------------------
with tabs[5]:
    st.subheader("⚡ Performance Benchmarking & Scaling")
    st.markdown(
        "Empirical benchmarks comparing starter datasets vs a large 100,000-order ingestion workload. "
        "Engineered with batched parameterized operations and transactional persistence."
    )

    bench_summary_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "evidence", "benchmark_summary.txt"
    )
    if os.path.exists(bench_summary_path):
        with open(bench_summary_path, "r", encoding="utf-8") as bf:
            st.text(bf.read())
    else:
        st.info(
            "Benchmark summary file will be visible once the 100,000-order benchmark script is executed."
        )

conn.close()
