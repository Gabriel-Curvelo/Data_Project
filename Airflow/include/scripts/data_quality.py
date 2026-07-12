import json
from datetime import datetime, timezone

from great_expectations.dataset import SparkDFDataset
from pyspark.sql import functions as F

from config import SILVER_PATH, DQ_REPORT_PATH
from spark_session import get_spark_session


VALID_TRANSACTION_TYPES = [
    "purchase",
    "sale",
    "transfer",
]

VALID_PURCHASE_PATTERNS = [
    "focused",
    "high_value",
    "random",
]

VALID_AGE_GROUPS = [
    "new",
    "established",
    "veteran",
    "none",
]

VALID_ANOMALY_LEVELS = [
    "low_risk",
    "moderate_risk",
    "high_risk",
]

REQUIRED_COLUMNS = [
    "timestamp",
    "sending_address",
    "receiving_address",
    "amount",
    "transaction_type",
    "location_region",
    "ip_prefix",
    "login_frequency",
    "session_duration",
    "purchase_pattern",
    "age_group",
    "risk_score",
    "anomaly",
]


def get_failed_expectations(validation_result: dict) -> list:
    failed_expectations = []

    for result in validation_result["results"]:
        if not result["success"]:
            config = result["expectation_config"]

            failed_expectations.append(
                {
                    "expectation": config["expectation_type"],
                    "column": config["kwargs"].get("column"),
                    "unexpected_count": result["result"].get(
                        "unexpected_count",
                        0,
                    ),
                    "unexpected_percent": result["result"].get(
                        "unexpected_percent",
                        0,
                    ),
                }
            )

    return failed_expectations


def run_data_quality():
    spark = get_spark_session("data-quality")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    df_silver = spark.read.parquet(SILVER_PATH)

    # Encapsula o DataFrame Spark para executar Expectations
    ge_df = SparkDFDataset(df_silver)

    # Schema: valida se todas as colunas necessárias existem
    ge_df.expect_table_columns_to_match_set(
        REQUIRED_COLUMNS + ["ingestion_timestamp"]
    )

    # Campos obrigatórios
    for column in REQUIRED_COLUMNS:
        ge_df.expect_column_values_to_not_be_null(column)

    # Tipos principais
    ge_df.expect_column_values_to_be_of_type(
        "timestamp",
        "TimestampType",
    )

    ge_df.expect_column_values_to_be_of_type(
        "amount",
        "DecimalType",
    )

    ge_df.expect_column_values_to_be_of_type(
        "risk_score",
        "DecimalType",
    )

    ge_df.expect_column_values_to_be_of_type(
        "login_frequency",
        "IntegerType",
    )

    ge_df.expect_column_values_to_be_of_type(
        "session_duration",
        "IntegerType",
    )

    # Valores de domínio
    ge_df.expect_column_values_to_be_in_set(
        "transaction_type",
        VALID_TRANSACTION_TYPES,
    )

    ge_df.expect_column_values_to_be_in_set(
        "purchase_pattern",
        VALID_PURCHASE_PATTERNS,
    )

    ge_df.expect_column_values_to_be_in_set(
        "age_group",
        VALID_AGE_GROUPS,
    )

    ge_df.expect_column_values_to_be_in_set(
        "anomaly",
        VALID_ANOMALY_LEVELS,
    )

    # Regras numéricas
    ge_df.expect_column_values_to_be_between(
        "amount",
        min_value=0,
    )

    ge_df.expect_column_values_to_be_between(
        "risk_score",
        min_value=0,
        max_value=100,
    )

    ge_df.expect_column_values_to_be_between(
        "login_frequency",
        min_value=0,
    )

    ge_df.expect_column_values_to_be_between(
        "session_duration",
        min_value=1,
    )

    # Formato dos endereços Ethereum: 0x + 40 caracteres hexadecimais
    ge_df.expect_column_values_to_match_regex(
        "sending_address",
        r"^0x[a-f0-9]{40}$",
    )

    ge_df.expect_column_values_to_match_regex(
        "receiving_address",
        r"^0x[a-f0-9]{40}$",
    )

    # Prefixos de IP presentes no dataset, como 192.0, 172.16 e 192.168
    ge_df.expect_column_values_to_match_regex(
        "ip_prefix",
        r"^(\d{1,3})(\.\d{1,3}){1,3}$",
    )

    # Evita linhas integralmente duplicadas
    ge_df.expect_compound_columns_to_be_unique(
        [
            "timestamp",
            "sending_address",
            "receiving_address",
            "amount",
            "transaction_type",
        ]
    )

    # Executa todas as Expectations registradas
    validation_result = ge_df.validate(
        result_format={
            "result_format": "SUMMARY",
            "partial_unexpected_count": 10,
        }
    )

    total_records = df_silver.count()
    failed_expectations = get_failed_expectations(validation_result)

    # Métricas complementares para o relatório Gold
    anomaly_metrics = (
        df_silver
        .groupBy("anomaly")
        .count()
        .withColumnRenamed("count", "records")
        .collect()
    )

    anomaly_distribution = {
        row["anomaly"]: row["records"]
        for row in anomaly_metrics
    }

    dq_report = {
        "execution_timestamp_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "total_records": total_records,
        "validation_success": validation_result["success"],
        "total_expectations": len(validation_result["results"]),
        "successful_expectations": (
            len(validation_result["results"])
            - len(failed_expectations)
        ),
        "failed_expectations_count": len(failed_expectations),
        "failed_expectations": failed_expectations,
        "anomaly_distribution": anomaly_distribution,
    }

    report_json = json.dumps(
        dq_report,
        ensure_ascii=False,
        default=str,
    )

    (
        spark.createDataFrame([(report_json,)], ["metrics_json"])
        .coalesce(1)
        .write
        .mode("overwrite")
        .text(DQ_REPORT_PATH)
    )

    print(report_json)

    if total_records == 0:
        raise ValueError(
            "Data Quality reprovada: a Silver Layer está vazia."
        )

    if not validation_result["success"]:
        raise ValueError(
            "Data Quality reprovada: uma ou mais "
            "expectations do Great Expectations falharam."
        )

    spark.stop()


if __name__ == "__main__":
    run_data_quality()