from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# default_args:
# conjunto de parâmetros padrão aplicados às tasks da DAG,
# evitando repetição de configuração em cada operador
default_args = {
    "owner": "gabriel_curvelo",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# Definição da DAG principal
with DAG(
    dag_id="data_engineering_pipeline",
    description="Pipeline Medallion: Bronze, Silver, Gold e Data Quality",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pyspark", "minio", "medallion"],
) as dag:

    # Task 1 - Bronze ingestion:
    # executa o script responsável por carregar o arquivo bruto
    # para a camada Bronze no MinIO
    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion",
        bash_command=(
            "python /usr/local/airflow/include/scripts/"
            "bronze_ingestion.py"
        ),
    )
    
    # Task 2 - Silver transformation:
    # executa o script PySpark que lê a Bronze,
    # aplica limpeza, tipagem e padronização,
    # e grava a camada Silver
    silver_transformation = BashOperator(
        task_id="silver_transformation",
        bash_command=(
            "python /usr/local/airflow/include/scripts/"
            "silver_transformation.py"
        ),
    )

    # Task 3 - Data quality validation:
    # executa o script de qualidade de dados,
    # que usa Great Expectations + regras adicionais em PySpark
    # para gerar o relatório de qualidade na camada Gold
    data_quality_validation = BashOperator(
        task_id="data_quality_validation",
        bash_command=(
            "python /usr/local/airflow/include/scripts/"
            "data_quality.py"
        ),
    )

    # Task 4 - Gold transformation:
    # executa o script que gera as tabelas analíticas da camada Gold
    # a partir da Silver validada
    gold_transformation = BashOperator(
        task_id="gold_transformation",
        bash_command=(
            "python /usr/local/airflow/include/scripts/"
            "gold_transformation.py"
        ),
    )

    # Definição das dependências entre tasks usando o operador >>
    #
    # Ordem do fluxo:
    # 1. bronze_ingestion
    # 2. silver_transformation
    # 3. data_quality_validation
    # 4. gold_transformation
    bronze_ingestion >> silver_transformation
    silver_transformation >> data_quality_validation
    data_quality_validation >> gold_transformation