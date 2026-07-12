from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "gabriel_curvelo",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="data_engineering_pipeline",
    description="Pipeline Medallion: Bronze, Silver, Gold e Data Quality",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pyspark", "minio", "medallion"],
) as dag:

    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion",
        bash_command=(
            "python /usr/local/airflow/include/scripts/"
            "bronze_ingestion.py"
        ),
    )

    silver_transformation = BashOperator(
        task_id="silver_transformation",
        bash_command=(
            "python /usr/local/airflow/include/scripts/"
            "silver_transformation.py"
        ),
    )

    data_quality_validation = BashOperator(
        task_id="data_quality_validation",
        bash_command=(
            "python /usr/local/airflow/include/scripts/"
            "data_quality.py"
        ),
    )

    gold_transformation = BashOperator(
        task_id="gold_transformation",
        bash_command=(
            "python /usr/local/airflow/include/scripts/"
            "gold_transformation.py"
        ),
    )

    bronze_ingestion >> silver_transformation
    silver_transformation >> data_quality_validation
    data_quality_validation >> gold_transformation