import os

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://host.docker.internal:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "datalake")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "datalake")

BRONZE_BUCKET = "bronzelayer"
SILVER_BUCKET = "silverlayer"
GOLD_BUCKET = "goldlayer"

RAW_FILE_NAME = "tb_fraud_credit.csv"

BRONZE_PATH = f"s3a://{BRONZE_BUCKET}/fraud_credit/{RAW_FILE_NAME}"
SILVER_PATH = f"s3a://{SILVER_BUCKET}/fraud_credit/"
GOLD_RISK_SCORE_PATH = (
    f"s3a://{GOLD_BUCKET}/location_region_risk_score/"
)
GOLD_TOP_ADDRESS_PATH = (
    f"s3a://{GOLD_BUCKET}/top_receiving_address_sales/"
)
FRAUD_CREDIT_PATH = (
    f"s3a://{GOLD_BUCKET}/fraud_credit/"
)
DQ_REPORT_PATH = f"s3a://{GOLD_BUCKET}/data_quality/"