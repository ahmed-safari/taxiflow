from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "Ahmed",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "nyc_taxi_etl",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    description="NYC Green Taxi ETL: Spark -> MinIO -> Postgres",
) as dag:

    extract_to_minio = BashOperator(
        task_id="extract_to_minio",
        bash_command=(
            "docker exec spark-master "
            "spark-submit /opt/spark/work-dir/extract_to_minio.py"
        ),
    )

    transform_load_postgres = BashOperator(
        task_id="transform_load_postgres",
        bash_command=(
            "docker exec spark-master "
            "spark-submit /opt/spark/work-dir/transform_load_postgres.py"
        ),
    )

    extract_to_minio >> transform_load_postgres
