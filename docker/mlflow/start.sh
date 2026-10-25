#!/bin/bash
set -e

pip install mlflow[extras]==3.16.0 psycopg2-binary boto3 --quiet

export MLFLOW_SERVER_ALLOWED_HOSTS="*"

mlflow server \
  --backend-store-uri "postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/mlflow_db" \
  --default-artifact-root "s3://olist/mlflow/artifacts" \
  --host 0.0.0.0 \
  --port 5001
