from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'nectar',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 1),
    'email_on_failure': True,
    'email': ['admin@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'iot_pipeline_dag',
    default_args=default_args,
    description='Nectar IoT Pipeline with local Spark',
    schedule_interval='*/2 * * * *',
    catchup=False,
    max_active_runs=1,
) as dag:

    # Start streaming jobs (only if not running)
    start_streaming = BashOperator(
        task_id='start_streaming',
        bash_command='''
            if ! pgrep -f "bronze_streaming.py"; then
                spark-submit --packages io.delta:delta-core_2.12:3.0.0 /opt/bitnami/spark/data/streaming/bronze_streaming.py &
            fi
            if ! pgrep -f "silver_streaming.py"; then
                spark-submit --packages io.delta:delta-core_2.12:3.0.0 /opt/bitnami/spark/data/streaming/silver_streaming.py &
            fi
        ''',
    )

    run_gold = BashOperator(
        task_id='run_gold_batch',
        bash_command='spark-submit --packages io.delta:delta-core_2.12:3.0.0 /opt/bitnami/spark/data/streaming/gold_batch.py',
    )

    load_postgres = BashOperator(
        task_id='load_postgres',
        bash_command='spark-submit --packages io.delta:delta-core_2.12:3.0.0 /opt/bitnami/spark/data/scripts/load_postgres.py',
    )

    start_streaming >> run_gold >> load_postgres
