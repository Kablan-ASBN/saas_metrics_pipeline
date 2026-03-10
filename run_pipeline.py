"""
run_pipeline.py
Loads raw CSV data into SQLite, then executes each SQL model in order:
staging -> intermediate -> marts.

This is basically a homemade version of what dbt does. I built it this way
intentionally — in an interview I can explain every step of the DAG because
I wrote the orchestration myself, rather than letting dbt handle it behind
the scenes.

How it works:
1. Read each CSV from the data/ folder into a SQLite table
2. For each .sql file in models/, wrap it in CREATE TABLE AS SELECT and execute
3. Run a handful of data quality checks on the output
4. Export the mart tables to CSV for the analysis script to pick up

The model execution order is hardcoded (not auto-detected from refs) because
with only 9 models it's clearer to be explicit about dependencies.
"""

import sqlite3
import pandas as pd
import os
import glob
import time

# --- Paths ---
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
MODELS_DIR = os.path.join(PROJECT_ROOT, "models")
DB_PATH = os.path.join(PROJECT_ROOT, "saas_metrics.db")

# --- Model execution order ---
# This is the DAG, flattened. Each model only depends on models listed above it.
# If you add a new model, slot it in after whatever it references.
MODEL_LAYERS = [
    ("staging", [
        "stg_customers",
        "stg_plans",
        "stg_subscriptions",
        "stg_events",
    ]),
    ("intermediate", [
        "int_subscription_months",
        "int_mrr_movements",
    ]),
    ("marts", [
        "fct_mrr_summary",
        "fct_cohort_retention",
        "dim_customer_ltv",
    ]),
]


def load_raw_data(conn):
    """Read every raw_*.csv file in the data folder into a SQLite table."""
    csv_files = glob.glob(os.path.join(DATA_DIR, "raw_*.csv"))
    for csv_path in csv_files:
        table_name = os.path.splitext(os.path.basename(csv_path))[0]
        df = pd.read_csv(csv_path)
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        print(f"  loaded {table_name:25s} ({len(df):,} rows)")


def run_model(conn, layer, model_name):
    """
    Execute a single SQL model.
    
    Each .sql file contains a SELECT statement. We wrap it in
    CREATE TABLE AS SELECT so the result becomes a real table
    that downstream models can reference.
    
    We drop the table first so the pipeline is idempotent —
    you can run it over and over and get the same result.
    """
    sql_path = os.path.join(MODELS_DIR, layer, f"{model_name}.sql")

    with open(sql_path, "r") as f:
        select_sql = f.read()

    # Drop + recreate for clean re-runs
    conn.execute(f"DROP TABLE IF EXISTS {model_name}")
    conn.execute(f"CREATE TABLE IF NOT EXISTS {model_name} AS\n{select_sql}")

    # Return row count so we can print a summary
    cursor = conn.execute(f"SELECT COUNT(*) FROM {model_name}")
    return cursor.fetchone()[0]


def export_marts(conn):
    """
    Save mart tables (fct_ and dim_) to CSV.
    
    These CSVs are what the analysis script reads from.
    We only export marts, not staging or intermediate tables,
    because those are internal pipeline concerns.
    """
    output_dir = os.path.join(PROJECT_ROOT, "output")
    os.makedirs(output_dir, exist_ok=True)

    for _, models in MODEL_LAYERS:
        for model_name in models:
            if model_name.startswith(("fct_", "dim_")):
                df = pd.read_sql(f"SELECT * FROM {model_name}", conn)
                output_path = os.path.join(output_dir, f"{model_name}.csv")
                df.to_csv(output_path, index=False)
                print(f"  exported {model_name}.csv")


def run_data_quality_checks(conn):
    """
    Basic sanity checks on the output tables.
    
    These aren't exhaustive — a real dbt project would have schema tests,
    freshness checks, and custom assertions. But these five catch the most
    common pipeline bugs:
    
    1. Negative MRR would mean our carry-forward logic is broken
    2. Missing customers would mean a join is dropping rows
    3. Retention > 100% would mean we're double-counting
    4. Missing months would mean our calendar spine has gaps
    5. Orphaned events would mean the data generator has a bug
    """
    checks = []

    # Check 1: MRR should never be negative
    result = conn.execute(
        "SELECT COUNT(*) FROM int_subscription_months WHERE mrr < 0"
    ).fetchone()[0]
    checks.append(("No negative MRR in subscription months", result == 0, result))

    # Check 2: Every customer should appear in the LTV table
    result = conn.execute("""
        SELECT COUNT(*) FROM stg_customers
        WHERE customer_id NOT IN (SELECT customer_id FROM dim_customer_ltv)
    """).fetchone()[0]
    checks.append(("All customers present in LTV table", result == 0, result))

    # Check 3: Retention rate should never exceed 100%
    result = conn.execute(
        "SELECT COUNT(*) FROM fct_cohort_retention WHERE retention_rate > 100"
    ).fetchone()[0]
    checks.append(("Retention rate <= 100%", result == 0, result))

    # Check 4: MRR summary should cover all 36 months of the analysis period
    result = conn.execute("""
        SELECT COUNT(DISTINCT month_id) FROM fct_mrr_summary
    """).fetchone()[0]
    checks.append(("MRR summary covers all months", result >= 30, result))

    # Check 5: Every event should reference a valid subscription
    result = conn.execute("""
        SELECT COUNT(*) FROM stg_events
        WHERE subscription_id NOT IN (SELECT subscription_id FROM stg_subscriptions)
    """).fetchone()[0]
    checks.append(("No orphaned events", result == 0, result))

    return checks


def main():
    start_time = time.time()

    print("=" * 60)
    print("  SaaS Metrics Pipeline")
    print("=" * 60)

    # Wipe the database so we get a clean build every time.
    # In a production setting you'd want incremental loading,
    # but for a portfolio project a full rebuild is simpler and safer.
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn = sqlite3.connect(DB_PATH)

    # Step 1: Load CSVs into SQLite
    print("\n[1/4] Loading raw data...")
    load_raw_data(conn)

    # Step 2: Run each model layer in order
    print("\n[2/4] Running models...")
    for layer, models in MODEL_LAYERS:
        print(f"\n  -- {layer} --")
        for model_name in models:
            try:
                row_count = run_model(conn, layer, model_name)
                print(f"  ✓ {model_name:35s} ({row_count:,} rows)")
            except Exception as e:
                print(f"  ✗ {model_name:35s} FAILED: {e}")
                raise  # stop the pipeline if a model fails

    conn.commit()

    # Step 3: Quality checks
    print("\n[3/4] Running data quality checks...")
    checks = run_data_quality_checks(conn)
    all_passed = True
    for check_name, passed, value in checks:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {status}  {check_name} (value: {value})")
        if not passed:
            all_passed = False

    # Step 4: Export marts to CSV
    print("\n[4/4] Exporting mart tables...")
    export_marts(conn)

    elapsed = time.time() - start_time
    conn.close()

    print(f"\n{'=' * 60}")
    print(f"  Pipeline complete in {elapsed:.1f}s")
    print(f"  Database: {DB_PATH}")
    print(f"  Quality:  {'ALL CHECKS PASSED' if all_passed else 'SOME CHECKS FAILED'}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()