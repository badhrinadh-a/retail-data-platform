from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['data-engineering-alerts@example.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'streaming_ingestion',
    default_args=default_args,
    description='Start Kafka to Bronze streaming ingestion',
    schedule_interval=None,
    catchup=False,
    tags=['bronze', 'streaming'],
) as dag:

    # We use a bash operator to submit to the spark master running in docker
    # since we would need a proper spark submit environment
    submit_spark_job = BashOperator(
        task_id='submit_streaming_ingestion',
        bash_command='''
        export PYTHONPATH=/opt/airflow && \
        spark-submit \
            --master spark://spark-master:7077 \
            --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
            /opt/airflow/src/spark/streaming_ingestion.py
        '''
    )
