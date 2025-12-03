"""
NYC Green Taxi ETL DAG
=====================
This DAG orchestrates the ETL pipeline for NYC Green Taxi data:
1. Extract: Download parquet data from NYC TLC and store in MinIO (Data Lake)
2. Transform & Load: Apply transformations using Spark and load to PostgreSQL (Data Warehouse)

The pipeline uses Docker exec to submit Spark jobs to the Spark cluster.

Author: Ahmed Yousef
Course: DSAI5102 Data Architecture & Engineering
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments for all tasks
default_args = {
    "owner": "Ahmed",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="nyc_taxi_etl",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    description="NYC Green Taxi ETL Pipeline: Extract to MinIO → Transform with Spark → Load to PostgreSQL",
    tags=["etl", "spark", "nyc-taxi", "data-engineering"],
) as dag:
    
    # Task 1: Extract data from NYC TLC and store in MinIO
    extract_to_minio = BashOperator(
        task_id="extract_to_minio",
        bash_command=(
            "docker exec spark-master "
            "spark-submit "
            "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
            "--conf spark.hadoop.fs.s3a.access.key=minioadmin "
            "--conf spark.hadoop.fs.s3a.secret.key=minioadmin "
            "--conf spark.hadoop.fs.s3a.path.style.access=true "
            "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
            "/opt/spark/work-dir/extract_to_minio.py"
        ),
    )
    
    # Task 2: Transform data and load to PostgreSQL
    transform_load_postgres = BashOperator(
        task_id="transform_load_postgres",
        bash_command=(
            "docker exec spark-master "
            "spark-submit "
            "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
            "--conf spark.hadoop.fs.s3a.access.key=minioadmin "
            "--conf spark.hadoop.fs.s3a.secret.key=minioadmin "
            "--conf spark.hadoop.fs.s3a.path.style.access=true "
            "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
            "/opt/spark/work-dir/transform_load_postgres.py"
        ),
    )
    
    # Define task dependencies
    extract_to_minio >> transform_load_postgres
