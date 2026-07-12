from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    TimestampType,
)

from config import BRONZE_PATH, SILVER_PATH
from spark_session import get_spark_session


def transform_to_silver():
    spark = get_spark_session("bees-silver-transformation")

    # Garante consistência independentemente do timezone do container
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    df_bronze = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(BRONZE_PATH)
    )

    df_silver = (
        df_bronze
        .select(
            F.trim(F.col("timestamp")).alias("timestamp_epoch"),
            F.lower(F.trim(F.col("sending_address"))).alias(
                "sending_address"
            ),
            F.lower(F.trim(F.col("receiving_address"))).alias(
                "receiving_address"
            ),
            F.trim(F.col("amount")).alias("amount_raw"),
            F.lower(F.trim(F.col("transaction_type"))).alias(
                "transaction_type"
            ),
            F.initcap(F.trim(F.col("location_region"))).alias(
                "location_region"
            ),
            F.trim(F.col("ip_prefix")).cast(StringType()).alias(
                "ip_prefix"
            ),
            F.trim(F.col("login_frequency")).alias(
                "login_frequency_raw"
            ),
            F.trim(F.col("session_duration")).alias(
                "session_duration_raw"
            ),
            F.lower(F.trim(F.col("purchase_pattern"))).alias(
                "purchase_pattern"
            ),
            F.lower(F.trim(F.col("age_group"))).alias(
                "age_group"
            ),
            F.trim(F.col("risk_score")).alias("risk_score_raw"),
            F.lower(F.trim(F.col("anomaly"))).alias("anomaly"),
        )
        .withColumn(
            "timestamp",
            F.to_timestamp(
                F.from_unixtime(
                    F.col("timestamp_epoch").cast("long")
                )
            )
        )
        .withColumn(
            "amount",
            F.regexp_replace(
                F.col("amount_raw"),
                ",",
                "."
            ).cast(DecimalType(18, 2))
        )
        .withColumn(
            "login_frequency",
            F.col("login_frequency_raw").cast(IntegerType())
        )
        .withColumn(
            "session_duration",
            F.col("session_duration_raw").cast(IntegerType())
        )
        .withColumn(
            "risk_score",
            F.regexp_replace(
                F.col("risk_score_raw"),
                ",",
                "."
            ).cast(DecimalType(5, 2))
        )
        .withColumn(
            "ingestion_timestamp",
            F.current_timestamp()
        )
        .drop(
            "timestamp_epoch",
            "amount_raw",
            "login_frequency_raw",
            "session_duration_raw",
            "risk_score_raw",
        )
        .dropDuplicates()
    )

    # Registros que efetivamente podem seguir para a Silver confiável
    df_valid = (
        df_silver
        .filter(F.col("timestamp").isNotNull())
        .filter(F.col("sending_address").isNotNull())
        .filter(F.col("receiving_address").isNotNull())
        .filter(F.col("amount").isNotNull() & (F.col("amount") >= 0))
        .filter(F.col("transaction_type").isin(
            "purchase",
            "sale",
            "transfer",
        ))
        .filter(F.col("location_region").isNotNull())
        .filter(
            F.col("risk_score").isNotNull()
            & F.col("risk_score").between(0, 100)
        )
        .filter(
            F.col("login_frequency").isNotNull()
            & (F.col("login_frequency") >= 0)
        )
        .filter(
            F.col("session_duration").isNotNull()
            & (F.col("session_duration") > 0)
        )
        .filter(F.col("anomaly").isin(
            "low_risk",
            "moderate_risk",
            "high_risk",
        ))
    )

    (
        df_valid.write
        .mode("overwrite")
        .partitionBy("transaction_type")
        .parquet(SILVER_PATH)
    )

    print(f"Quantidade de registros Bronze: {df_bronze.count()}")
    print(f"Quantidade de registros Silver válidos: {df_valid.count()}")

    spark.stop()


if __name__ == "__main__":
    transform_to_silver()