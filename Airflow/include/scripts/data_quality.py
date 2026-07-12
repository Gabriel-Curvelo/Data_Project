"""O objetivo do script é criar um dataquality report da camada Silver, validando tipos 
de dados, valores e regras de negócio."""

import json
from datetime import datetime, timezone

from great_expectations.dataset import SparkDFDataset
from pyspark.sql import functions as F

from config import SILVER_PATH, DQ_REPORT_PATH
from spark_session import get_spark_session

# Domínio permitido para as colunas
VALID_TRANSACTION_TYPES = [
    "purchase",
    "sale",
    "transfer",
]

VALID_PURCHASE_PATTERNS = [
    "focused",
    "high_value",
    "random",
    "none",
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

VALID_LOCATION_REGIONS = [
    "Africa",
    "Antarctica",
    "Asia",
    "Europe",
    "North America",
    "Oceania",
    "South America",
]

# Conjunto de colunas obrigatórias esperadas na Silver
# Será usado tanto para validar schema quanto campos críticos não nulos
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

# Retorna a lista consolidada de falhas
def get_failed_expectations(validation_result: dict) -> list:
    failed_expectations = []

    for result in validation_result["results"]:
        if not result["success"]:
            config = result.get("expectation_config", {})
            result_data = result.get("result", {})

            failed_expectations.append(
                {
                    "expectation": config.get("expectation_type"),
                    "column": config.get("kwargs", {}).get("column"),
                    "unexpected_count": result_data.get(
                        "unexpected_count",
                        0,
                    ),
                    "unexpected_percent": result_data.get(
                        "unexpected_percent",
                        0,
                    ),
                    "partial_unexpected_list": result_data.get(
                        "partial_unexpected_list",
                        [],
                    ),
                }
            )

    return failed_expectations

# Coleta valores distintos de uma coluna para exibir amostras no relatório
# útil para entender rapidamente quais categorias inválidas apareceram
def collect_distinct_values(df, column_name: str, limit: int = 20) -> list:
    rows = (
        df.select(column_name)
        .distinct()
        .limit(limit)
        .collect()
    )
    return [row[column_name] for row in rows]


def run_data_quality():
    spark = get_spark_session("data-quality")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    df_silver = spark.read.parquet(SILVER_PATH)

    total_records = df_silver.count()

    if total_records == 0:
        raise ValueError(
            "Data Quality reprovada: a Silver Layer está vazia."
        )

    # Normalização prévia dos dados:
    # padroniza caixa, remove espaços extras e ajusta formato textual
    # Isso reduz falsos positivos durante as validações
    df_normalized = (
        df_silver
        .withColumn(
            "transaction_type",
            F.lower(F.trim(F.col("transaction_type"))),
        )
        .withColumn(
            "purchase_pattern",
            F.lower(F.trim(F.col("purchase_pattern"))),
        )
        .withColumn(
            "age_group",
            F.lower(F.trim(F.col("age_group"))),
        )
        .withColumn(
            "anomaly",
            F.lower(F.trim(F.col("anomaly"))),
        )
        .withColumn(
            "sending_address",
            F.lower(F.trim(F.col("sending_address"))),
        )
        .withColumn(
            "receiving_address",
            F.lower(F.trim(F.col("receiving_address"))),
        )
        .withColumn(
            "ip_prefix",
            F.trim(F.col("ip_prefix")),
        )
        .withColumn(
            "location_region",
            F.initcap(F.trim(F.col("location_region"))),
        )
    )

    ge_df = SparkDFDataset(df_normalized)

    # Validação estrutural:
    # garante que o conjunto de colunas do DataFrame corresponda ao esperado
    # ingestion_timestamp também é esperado por ter sido criado na Silver
    ge_df.expect_table_columns_to_match_set(
        REQUIRED_COLUMNS + ["ingestion_timestamp"]
    )
    # cada coluna obrigatória deve conter valores não nulos
    for column in REQUIRED_COLUMNS:
        ge_df.expect_column_values_to_not_be_null(column)

    # Garante que location_region não tenha string vazia ou apenas espaços
    ge_df.expect_column_values_to_not_match_regex(
        "location_region",
        r"^\s*$",
    )

    # Valida tipos principais esperados no schema
    ge_df.expect_column_values_to_be_of_type(
        "timestamp",
        "TimestampType",
    )

    ge_df.expect_column_values_to_be_of_type(
        "amount",
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

    ge_df.expect_column_values_to_be_in_set(
        "location_region",
        VALID_LOCATION_REGIONS,
    )

    ge_df.expect_column_distinct_values_to_be_in_set(
        "location_region",
        VALID_LOCATION_REGIONS,
    )

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

    ge_df.expect_column_value_lengths_to_equal(
        "sending_address",
        42,
    )

    ge_df.expect_column_value_lengths_to_equal(
        "receiving_address",
        42,
    )

    ge_df.expect_column_values_to_match_regex(
        "sending_address",
        r"^0x[a-f0-9]{40}$",
    )

    ge_df.expect_column_values_to_match_regex(
        "receiving_address",
        r"^0x[a-f0-9]{40}$",
    )

    ge_df.expect_column_values_to_match_regex(
        "ip_prefix",
        r"^(\d{1,3})(\.\d{1,3}){1,3}$",
    )

    # Garante unicidade composta para evitar duplicidade transacional
    # A combinação dessas colunas deve identificar unicamente uma transação
    ge_df.expect_compound_columns_to_be_unique(
        [
            "timestamp",
            "sending_address",
            "receiving_address",
            "amount",
            "transaction_type",
        ]
    )

    # Executa todas as expectations registradas
    # SUMMARY retorna uma visão resumida com amostras de valores inesperados
    validation_result = ge_df.validate(
        result_format={
            "result_format": "SUMMARY",
            "partial_unexpected_count": 10,
        }
    )
    # Extrai apenas as expectations que falharam para simplificar o relatório
    failed_expectations = get_failed_expectations(validation_result)

    # Regras condicionais implementadas com PySpark puro:

    invalid_transfer_purchase_pattern_df = df_normalized.filter(
        (F.col("transaction_type") == "transfer")
        & (F.col("purchase_pattern") != "none")
    )

    invalid_transfer_age_group_df = df_normalized.filter(
        (F.col("transaction_type") == "transfer")
        & (F.col("age_group") != "none")
    )

    invalid_high_risk_df = df_normalized.filter(
        (F.col("anomaly") == "high_risk")
        & ((F.col("risk_score") < 70) | (F.col("risk_score") > 100))
    )

    invalid_moderate_risk_df = df_normalized.filter(
        (F.col("anomaly") == "moderate_risk")
        & (
            (F.col("risk_score") < 40)
            | (F.col("risk_score") >= 70)
        )
    )

    invalid_low_risk_df = df_normalized.filter(
        (F.col("anomaly") == "low_risk")
        & ((F.col("risk_score") < 0) | (F.col("risk_score") >= 40))
    )

    invalid_location_region_df = df_normalized.filter(
        ~F.col("location_region").isin(VALID_LOCATION_REGIONS)
    )

    conditional_failures = {
        "transfer_purchase_pattern_none_rule": {
            "invalid_count": invalid_transfer_purchase_pattern_df.count(),
            "sample_values": collect_distinct_values(
                invalid_transfer_purchase_pattern_df,
                "purchase_pattern",
            ),
        },
        "transfer_age_group_none_rule": {
            "invalid_count": invalid_transfer_age_group_df.count(),
            "sample_values": collect_distinct_values(
                invalid_transfer_age_group_df,
                "age_group",
            ),
        },
        "high_risk_score_range_rule": {
            "invalid_count": invalid_high_risk_df.count(),
        },
        "moderate_risk_score_range_rule": {
            "invalid_count": invalid_moderate_risk_df.count(),
        },
        "low_risk_score_range_rule": {
            "invalid_count": invalid_low_risk_df.count(),
        },
        "location_region_domain_rule": {
            "invalid_count": invalid_location_region_df.count(),
            "sample_values": collect_distinct_values(
                invalid_location_region_df,
                "location_region",
            ),
        },
    }

    conditional_rules_success = all(
        rule["invalid_count"] == 0
        for rule in conditional_failures.values()
    )

    anomaly_metrics = (
        df_normalized
        .groupBy("anomaly")
        .count()
        .withColumnRenamed("count", "records")
        .collect()
    )

    anomaly_distribution = {
        row["anomaly"]: row["records"]
        for row in anomaly_metrics
    }

    region_metrics = (
        df_normalized
        .groupBy("location_region")
        .count()
        .withColumnRenamed("count", "records")
        .collect()
    )

    location_region_distribution = {
        row["location_region"]: row["records"]
        for row in region_metrics
    }

    duplicate_count = (
        df_normalized
        .groupBy(
            "timestamp",
            "sending_address",
            "receiving_address",
            "amount",
            "transaction_type",
        )
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    # Status geral:
    # só será True se o Great Expectations passar
    # e se todas as regras condicionais também passarem
    overall_success = (
        validation_result["success"]
        and conditional_rules_success
    )
    
    # Estrutura final do relatório
    # Reúne métricas gerais, falhas do GE, falhas condicionais e distribuições
    dq_report = {
        "execution_timestamp_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "total_records": total_records,
        "validation_success": validation_result["success"],
        "conditional_rules_success": conditional_rules_success,
        "overall_success": overall_success,
        "total_expectations": len(validation_result["results"]),
        "successful_expectations": (
            len(validation_result["results"])
            - len(failed_expectations)
        ),
        "failed_expectations_count": len(failed_expectations),
        "failed_expectations": failed_expectations,
        "conditional_failures": conditional_failures,
        "duplicate_count": duplicate_count,
        "anomaly_distribution": anomaly_distribution,
        "location_region_distribution": (
            location_region_distribution
        ),
        "allowed_location_regions": VALID_LOCATION_REGIONS,
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

    # Não levanta exceção: sempre deixa a DAG seguir.
    # O status da qualidade fica apenas no relatório.
    if not overall_success:
        print(
            "Data Quality WARNING: falhas em expectations "
            "do Great Expectations e/ou regras condicionais."
        )
    else:
        print("Data Quality OK: todas as regras foram atendidas.")

    spark.stop()


if __name__ == "__main__":
    run_data_quality()