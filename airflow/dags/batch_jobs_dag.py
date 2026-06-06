"""
Batch Processing Pipeline DAG

Orchestrates the full batch pipeline:
  1. Bronze → Silver transformation (MERGE upsert)
  2. Data quality validation on Silver layer
  3. Silver → Gold aggregation (MERGE upsert)
  4. Data quality validation on Gold layer

Quality checks use real validation logic (schema, nulls, duplicates,
value ranges) and will fail the pipeline if critical checks don't pass.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    # Fixed start_date — using datetime.today() is an Airflow anti-pattern
    # because it re-evaluates on every DAG parse (~30s), causing scheduling bugs
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['data-engineering-alerts@example.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Shared Spark packages to avoid duplication
SPARK_PACKAGES = ",".join([
    "io.delta:delta-core_2.12:4.2.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])

SPARK_SUBMIT_BASE = f"""
    export PYTHONPATH=/opt/airflow && \\
    spark-submit \\
        --master spark://spark-master:7077 \\
        --packages {SPARK_PACKAGES}
"""

with DAG(
    'batch_processing_pipeline',
    default_args=default_args,
    description='Run Silver and Gold transformations with quality gates',
    schedule_interval='@daily',
    catchup=False,
    tags=['silver', 'gold', 'batch', 'quality'],
    max_active_runs=1,
) as dag:

    # Step 1: Bronze → Silver MERGE upsert
    run_silver_transform = BashOperator(
        task_id='bronze_to_silver_orders',
        bash_command=f"""
        {SPARK_SUBMIT_BASE} \\
            /opt/airflow/src/spark/batch_transform.py
        """,
    )

    # Step 2: Validate Silver layer quality
    silver_quality_check = BashOperator(
        task_id='silver_data_quality_check',
        bash_command=f"""
        {SPARK_SUBMIT_BASE} \\
            /opt/airflow/src/quality/data_quality.py
        """,
        doc_md="""
        ### Silver Data Quality Gate
        Runs schema validation, null checks, duplicate detection, value range
        validation, and allowed value checks on the Silver orders table.
        **Fails the pipeline** if any ERROR-severity check fails.
        """,
    )

    # Step 3: Silver → Gold MERGE upsert
    run_gold_aggregate = BashOperator(
        task_id='silver_to_gold_metrics',
        bash_command=f"""
        {SPARK_SUBMIT_BASE} \\
            /opt/airflow/src/spark/batch_aggregate.py
        """,
    )

    # Step 4: Validate Gold layer quality
    gold_quality_check = BashOperator(
        task_id='gold_data_quality_check',
        bash_command=f"""
        {SPARK_SUBMIT_BASE} \\
            /opt/airflow/src/quality/data_quality.py
        """,
        doc_md="""
        ### Gold Data Quality Gate
        Validates the Gold daily sales summary: schema, nulls, revenue > 0,
        order counts > 0. **Fails the pipeline** if checks don't pass.
        """,
    )

    # Pipeline: transform → validate → aggregate → validate
    run_silver_transform >> silver_quality_check >> run_gold_aggregate >> gold_quality_check
