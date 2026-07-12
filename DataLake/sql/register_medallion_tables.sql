CREATE SCHEMA IF NOT EXISTS hive.gold;

CREATE TABLE IF NOT EXISTS hive.gold.fraud_credit (
    "timestamp" TIMESTAMP,
    sending_address VARCHAR,
    receiving_address VARCHAR,
    amount DECIMAL(18, 2),
    location_region VARCHAR,
    ip_prefix VARCHAR,
    login_frequency INTEGER,
    session_duration INTEGER,
    purchase_pattern VARCHAR,
    age_group VARCHAR,
    risk_score DECIMAL(5, 2),
    anomaly VARCHAR,
    ingestion_timestamp TIMESTAMP,
    transaction_type VARCHAR
)
WITH (
    external_location = 's3a://goldlayer/fraud_credit/',
    format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS hive.gold.location_region_risk_score (
    location_region VARCHAR,
    avg_risk_score DECIMAL(10, 4),
    total_transactions BIGINT,
    suspicious_transactions BIGINT
)
WITH (
    external_location = 's3a://goldlayer/location_region_risk_score/',
    format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS hive.gold.top_receiving_address_sales (
    receiving_address VARCHAR,
    amount DECIMAL(18, 2),
    "timestamp" TIMESTAMP
)
WITH (
    external_location = 's3a://goldlayer/top_receiving_address_sales/',
    format = 'PARQUET'
);