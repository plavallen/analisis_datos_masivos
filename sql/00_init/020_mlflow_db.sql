-- Base de datos del backend de MLflow (docker/mlflow/start.sh usa postgres:5432/mlflow_db).
-- Idempotente: solo la crea si no existe. CREATE DATABASE no admite IF NOT EXISTS
-- ni puede ir dentro de un bloque DO, por eso se usa \gexec de psql.

SELECT 'CREATE DATABASE mlflow_db'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'mlflow_db')
\gexec
