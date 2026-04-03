from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, "/opt/airflow/project")

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

with DAG(
    dag_id="ecommerce_pipeline",
    description="E-commerce data pipeline: extract → bronze → silver → gold → test",
    schedule_interval="0 6 * * *",   # runs daily at 6am
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["ecommerce", "dbt", "duckdb"],
) as dag:

    # ── Task 1: Extract from dummyjson API ───────────────────
    def run_extract(**context):
        from ingestion.extract import run as extract
        results = extract()
        # push file paths to XCom so load task can read them
        context["ti"].xcom_push(key="raw_files", value=results)
        print(f"Extracted: {results}")

    extract_task = PythonOperator(
        task_id="extract_from_api",
        python_callable=run_extract,
        provide_context=True,
    )

    # ── Task 2: Load raw data into Bronze ───────────────────
    def run_load(**context):
        from ingestion.load import run as load_bronze
        raw_files = context["ti"].xcom_pull(
            key="raw_files",
            task_ids="extract_from_api"
        )
        load_bronze(raw_files)
        print("Bronze load complete")

    load_task = PythonOperator(
        task_id="load_to_bronze",
        python_callable=run_load,
        provide_context=True,
    )

    # ── Task 3: Run dbt Silver models ───────────────────────
    dbt_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=(
            "cd /opt/airflow/project/dbt_project/ecommerce_dbt && "
            "dbt run --select silver --profiles-dir /opt/airflow/project/dbt_project"
        ),
    )

    # ── Task 4: Run dbt Gold models ─────────────────────────
    dbt_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=(
            "cd /opt/airflow/project/dbt_project/ecommerce_dbt && "
            "dbt run --select gold --profiles-dir /opt/airflow/project/dbt_project"
        ),
    )

    # ── Task 5: Run dbt tests ────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/project/dbt_project/ecommerce_dbt && "
            "dbt test --profiles-dir /opt/airflow/project/dbt_project"
        ),
    )

    # ── Task 6: Pipeline health check ───────────────────────
    def run_health_check(**context):
        import duckdb
        conn = duckdb.connect("/opt/airflow/project/data/ecommerce.duckdb")

        checks = {
            "bronze.products_raw": 100,
            "bronze.carts_raw":    10,
            "bronze.users_raw":    100,
            "silver.dim_products": 100,
            "silver.fct_carts":    10,
            "gold.category_performance": 1,
            "gold.top_products":   1,
        }

        failed = []
        for table, min_rows in checks.items():
            count = conn.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
            status = "OK" if count >= min_rows else "FAIL"
            print(f"  {status} | {table}: {count} rows (min: {min_rows})")
            if count < min_rows:
                failed.append(f"{table}: {count} rows < {min_rows}")

        conn.close()

        if failed:
            raise ValueError(f"Health check failed: {failed}")

        print("All health checks passed.")

    health_check = PythonOperator(
        task_id="pipeline_health_check",
        python_callable=run_health_check,
        provide_context=True,
    )

    # ── DAG dependency chain ─────────────────────────────────
    extract_task >> load_task >> dbt_silver >> dbt_gold >> dbt_test >> health_check