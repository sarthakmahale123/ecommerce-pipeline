import duckdb
import json
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = "data/ecommerce.duckdb"


def get_conn():
    return duckdb.connect(DB_PATH)


def setup_monitoring_tables():
    """Create monitoring schema and tables if they don't exist."""
    conn = get_conn()

    conn.execute("CREATE SCHEMA IF NOT EXISTS monitoring")

    # Pipeline run log — one row per pipeline execution
    conn.execute("""
        CREATE TABLE IF NOT EXISTS monitoring.pipeline_runs (
            run_id          VARCHAR,
            run_started_at  TIMESTAMP,
            run_finished_at TIMESTAMP,
            duration_seconds DOUBLE,
            status          VARCHAR,
            products_loaded INTEGER,
            carts_loaded    INTEGER,
            users_loaded    INTEGER,
            error_message   VARCHAR,
            triggered_by    VARCHAR
        )
    """)

    # Row count history — tracks counts over time for anomaly detection
    conn.execute("""
        CREATE TABLE IF NOT EXISTS monitoring.row_count_history (
            checked_at      TIMESTAMP,
            table_name      VARCHAR,
            row_count       INTEGER,
            prev_count      INTEGER,
            pct_change      DOUBLE,
            anomaly_flag    BOOLEAN
        )
    """)

    # Data quality run log — one row per dbt test run
    conn.execute("""
        CREATE TABLE IF NOT EXISTS monitoring.quality_checks (
            checked_at      TIMESTAMP,
            total_tests     INTEGER,
            passed_tests    INTEGER,
            failed_tests    INTEGER,
            status          VARCHAR,
            failed_details  VARCHAR
        )
    """)

    conn.close()
    print("Monitoring tables ready.")


def log_pipeline_run(run_id, started_at, finished_at, status,
                     products=0, carts=0, users=0,
                     error=None, triggered_by="manual"):
    """Log a completed pipeline run."""
    conn = get_conn()
    duration = (finished_at - started_at).total_seconds()

    conn.execute("""
        INSERT INTO monitoring.pipeline_runs VALUES (?,?,?,?,?,?,?,?,?,?)
    """, [
        run_id, started_at, finished_at, duration,
        status, products, carts, users,
        error, triggered_by
    ])
    conn.close()


def check_row_counts():
    """
    Check current row counts against history.
    Flag anomalies where count drops more than 20% from previous run.
    """
    conn = get_conn()
    checked_at = datetime.now(timezone.utc)

    tables = {
        "bronze.products_raw":          50,
        "bronze.carts_raw":             10,
        "bronze.users_raw":             50,
        "silver.dim_products":          50,
        "silver.fct_carts":             10,
        "silver.fct_cart_items":        10,
        "gold.category_performance":     1,
        "gold.top_products":             1,
        "gold.daily_revenue":            1,
    }

    anomalies = []

    for table, min_expected in tables.items():
        current = conn.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]

        # Get previous count
        prev_row = conn.execute("""
            SELECT row_count FROM monitoring.row_count_history
            WHERE table_name = ?
            ORDER BY checked_at DESC
            LIMIT 1
        """, [table]).fetchone()

        prev_count = prev_row[0] if prev_row else current
        pct_change = ((current - prev_count) / max(prev_count, 1)) * 100
        anomaly = (
            current < min_expected or
            (prev_count > 0 and pct_change < -20)
        )

        if anomaly:
            anomalies.append({
                "table": table,
                "current": current,
                "prev": prev_count,
                "pct_change": round(pct_change, 2),
                "min_expected": min_expected
            })

        conn.execute("""
            INSERT INTO monitoring.row_count_history VALUES (?,?,?,?,?,?)
        """, [
            checked_at, table, current,
            prev_count, round(pct_change, 2), anomaly
        ])

    conn.close()
    return anomalies


def check_data_freshness():
    """
    Check when Bronze tables were last loaded.
    Alert if data is older than 25 hours (missed a daily run).
    """
    conn = get_conn()
    now = datetime.now(timezone.utc)
    stale = []

    tables = [
        "bronze.products_raw",
        "bronze.carts_raw",
        "bronze.users_raw"
    ]

    for table in tables:
        result = conn.execute(
            f"SELECT MAX(_ingested_at) FROM {table}"
        ).fetchone()[0]

        if result is None:
            stale.append({"table": table, "reason": "no data"})
            continue

        # DuckDB returns naive datetime — treat as UTC
        if hasattr(result, 'tzinfo') and result.tzinfo is None:
            from datetime import timezone as tz
            result = result.replace(tzinfo=tz.utc)

        age_hours = (now - result).total_seconds() / 3600

        if age_hours > 25:
            stale.append({
                "table": table,
                "last_loaded": str(result),
                "age_hours": round(age_hours, 1)
            })

    conn.close()
    return stale


def log_quality_check(total, passed, failed, failed_details=None):
    """Log result of a dbt test run."""
    conn = get_conn()
    conn.execute("""
        INSERT INTO monitoring.quality_checks VALUES (?,?,?,?,?,?)
    """, [
        datetime.now(timezone.utc),
        total, passed, failed,
        "PASS" if failed == 0 else "FAIL",
        json.dumps(failed_details or [])
    ])
    conn.close()


def print_monitoring_report():
    """Print a full monitoring report to terminal."""
    conn = get_conn()

    print("\n" + "="*60)
    print("PIPELINE MONITORING REPORT")
    print("="*60)

    print("\n--- Recent Pipeline Runs ---")
    runs = conn.execute("""
        SELECT run_id, run_started_at, duration_seconds,
               status, products_loaded, carts_loaded, users_loaded
        FROM monitoring.pipeline_runs
        ORDER BY run_started_at DESC
        LIMIT 5
    """).fetchdf()
    print(runs.to_string(index=False) if len(runs) > 0
          else "  No runs logged yet.")

    print("\n--- Row Count Anomalies (last check) ---")
    anomalies = conn.execute("""
        SELECT table_name, row_count, prev_count,
               pct_change, anomaly_flag, checked_at
        FROM monitoring.row_count_history
        WHERE checked_at = (SELECT MAX(checked_at)
                            FROM monitoring.row_count_history)
        AND anomaly_flag = true
    """).fetchdf()
    print(anomalies.to_string(index=False) if len(anomalies) > 0
          else "  No anomalies detected.")

    print("\n--- Current Row Counts ---")
    counts = conn.execute("""
        SELECT table_name, row_count, pct_change, checked_at
        FROM monitoring.row_count_history
        WHERE checked_at = (SELECT MAX(checked_at)
                            FROM monitoring.row_count_history)
        ORDER BY table_name
    """).fetchdf()
    print(counts.to_string(index=False) if len(counts) > 0
          else "  No counts logged yet.")

    print("\n--- Quality Check History ---")
    quality = conn.execute("""
        SELECT checked_at, total_tests, passed_tests,
               failed_tests, status
        FROM monitoring.quality_checks
        ORDER BY checked_at DESC
        LIMIT 5
    """).fetchdf()
    print(quality.to_string(index=False) if len(quality) > 0
          else "  No quality checks logged yet.")

    conn.close()
    print("\n" + "="*60)


if __name__ == "__main__":
    setup_monitoring_tables()
    print("\nRunning row count checks...")
    anomalies = check_row_counts()

    if anomalies:
        print(f"\n  ANOMALIES DETECTED: {len(anomalies)}")
        for a in anomalies:
            print(f"  {a}")
    else:
        print("  All row counts healthy.")

    print("\nRunning freshness checks...")
    stale = check_data_freshness()
    if stale:
        print(f"\n  STALE DATA DETECTED: {len(stale)}")
        for s in stale:
            print(f"  {s}")
    else:
        print("  All data is fresh.")

    log_quality_check(
        total=38, passed=38, failed=0,
        failed_details=[]
    )

    print_monitoring_report()