import os
import boto3

from config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    BRONZE_BUCKET,
    RAW_FILE_NAME,
)

LOCAL_SOURCE_FILE = "/usr/local/airflow/include/data/df_fraud_credit.csv"


def upload_to_bronze():
    if not os.path.exists(LOCAL_SOURCE_FILE):
        raise FileNotFoundError(
            f"Arquivo não encontrado: {LOCAL_SOURCE_FILE}"
        )

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    s3.upload_file(
        Filename=LOCAL_SOURCE_FILE,
        Bucket=BRONZE_BUCKET,
        Key=f"fraud_credit/{RAW_FILE_NAME}",
    )

    print(
        f"Arquivo enviado para "
        f"s3://{BRONZE_BUCKET}/fraud_credit/{RAW_FILE_NAME}"
    )


if __name__ == "__main__":
    upload_to_bronze()