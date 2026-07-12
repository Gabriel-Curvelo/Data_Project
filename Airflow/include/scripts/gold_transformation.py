"""O objetivo do script é realizar a transformação dos dados da camada Silver para a camada 
Gold, aplicando agregações e cálculos de métricas."""

#Importações
from pyspark.sql import Window
from pyspark.sql import functions as F

from config import (
    SILVER_PATH,
    FRAUD_CREDIT_PATH,
    GOLD_RISK_SCORE_PATH,
    GOLD_TOP_ADDRESS_PATH,
)
from spark_session import get_spark_session


def create_gold_tables():
    spark = get_spark_session("gold-transformation")

    # Lê a camada Silver em formato Parquet, já tipada e validada
    df_silver = spark.read.parquet(SILVER_PATH)

    # Persiste a Silver como tabela Gold bruta (fraud_credit):
    # - útil como base analítica, mantendo o schema pronto para Trino/Hive
    (
        df_silver.write
        .mode("overwrite")
        .parquet(FRAUD_CREDIT_PATH)
    )

    # Tabela Gold 2: agregações por região (location_region)
    # Calcula:
    # - média do risk_score por região (avg_risk_score)
    # - total de transações por região (total_transactions)
    # - quantidade de transações suspeitas (anomaly != low_risk)
    # Em seguida ordena pela média de risk_score em ordem decrescente
    risk_score_by_region = (
        df_silver
        .groupBy("location_region")
        .agg(
            F.round(F.avg("risk_score"), 2).alias("avg_risk_score"),
            F.count("*").alias("total_transactions"),
            F.sum(
                F.when(F.col("anomaly") != "low_risk", 1).otherwise(0)
            ).alias("suspicious_transactions"),
        )
        .orderBy(F.col("avg_risk_score").desc())
    )

    (
        risk_score_by_region.write
        .mode("overwrite")
        .parquet(GOLD_RISK_SCORE_PATH)
    )

    sales_df = df_silver.filter(
        F.col("transaction_type") == "sale"
    )

    window_spec = (
        Window
        .partitionBy("receiving_address")
        .orderBy(F.col("timestamp").desc())
    )

    latest_sale_per_address = (
        sales_df
        .withColumn(
            "row_number",
            F.row_number().over(window_spec),
        )
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )

    # Tabela Gold 3: top 3 receiving_address por valor de amount
    # - Considera apenas a venda mais recente por endereço 
    # - Ordena por amount desc, depois timestamp desc 
    # - Seleciona apenas receiving_address, amount e timestamp
    # - Limita a 3 linhas (top 3)
    top_3_receiving_addresses = (
        latest_sale_per_address
        .select(
            "receiving_address",
            "amount",
            "timestamp",
        )
        .orderBy(
            F.col("amount").desc(),
            F.col("timestamp").desc(),
        )
        .limit(3)
    )

    (
        top_3_receiving_addresses.write
        .mode("overwrite")
        .parquet(GOLD_TOP_ADDRESS_PATH)
    )

    print("Tabela Gold 1 criada: limpa")
    print("Tabela Gold 2 criada: média de risk score por região")
    print("Tabela Gold 3 criada: top receiving addresses")

    spark.stop()


if __name__ == "__main__":
    create_gold_tables()