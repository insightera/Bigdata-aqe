-- Hive Metastore DB (default dari POSTGRES_DB)
-- airflow_db untuk Airflow

CREATE DATABASE airflow_db;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO admin;

CREATE DATABASE iceberg_catalog;
GRANT ALL PRIVILEGES ON DATABASE iceberg_catalog TO admin;

CREATE DATABASE superset_db;
GRANT ALL PRIVILEGES ON DATABASE superset_db TO admin;

-- metastore_db = default dari POSTGRES_DB
