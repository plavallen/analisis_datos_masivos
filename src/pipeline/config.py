import os
from pyspark.sql import SparkSession

PERIODO_INICIO = "2017-01"
PERIODO_FIN    = "2018-08"

BRONZE_BASE  = "s3a://olist/bronze"
SILVER_BASE  = "s3a://olist/silver"
GOLD_BASE    = "s3a://olist/gold"
WAREHOUSE    = "s3a://olist/warehouse"

N_MIN_GRUPO  = 30

IQR_MULT     = 3.0

TRAMOS = [
    (   50, "1_local (<50 km)"),
    (  300, "2_regional (50-300 km)"),
    ( 1000, "3_nacional_media (300-1000 km)"),
    ( None, "4_nacional_larga (>1000 km)"),
]

REGIONES = [
    ("AM", "Norte"), ("RR", "Norte"), ("AP", "Norte"), ("PA", "Norte"),
    ("TO", "Norte"), ("RO", "Norte"), ("AC", "Norte"),
    ("MA", "Nordeste"), ("PI", "Nordeste"), ("CE", "Nordeste"), ("RN", "Nordeste"),
    ("PE", "Nordeste"), ("PB", "Nordeste"), ("SE", "Nordeste"), ("AL", "Nordeste"),
    ("BA", "Nordeste"),
    ("MT", "Centro-Oeste"), ("MS", "Centro-Oeste"), ("GO", "Centro-Oeste"),
    ("DF", "Centro-Oeste"),
    ("SP", "Sudeste"), ("RJ", "Sudeste"), ("MG", "Sudeste"), ("ES", "Sudeste"),
    ("PR", "Sul"), ("SC", "Sul"), ("RS", "Sul"),
]

LAT_MIN, LAT_MAX = -34.0,  5.3
LNG_MIN, LNG_MAX = -74.0, -28.0

R_TIERRA_KM = 6371.0


def _env(key, default):
    return os.environ.get(key, default)


def get_spark(app_name="olist-pipeline"):
    pg_host = _env("POSTGRES_HOST", "postgres")
    pg_port = _env("POSTGRES_PORT", "5432")
    pg_db   = _env("POSTGRES_DB",   "olist_db")

    rustfs   = _env("RUSTFS_ENDPOINT",       "http://rustfs:9000")
    s3_key   = _env("AWS_ACCESS_KEY_ID",     "rustfsadmin")
    s3_sec   = _env("AWS_SECRET_ACCESS_KEY", "rustfsadmin123")

    jars = ",".join([
        "/opt/spark/extra-jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar",
        "/opt/spark/extra-jars/hadoop-aws-3.4.2.jar",
        "/opt/spark/extra-jars/aws-sdk-bundle-v2-2.29.52.jar",
        "/opt/spark/extra-jars/postgresql-42.7.4.jar",
    ])

    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars", jars)
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint",               rustfs)
        .config("spark.hadoop.fs.s3a.access.key",             s3_key)
        .config("spark.hadoop.fs.s3a.secret.key",             s3_sec)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake",           "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type",      "hadoop")
        .config("spark.sql.catalog.lake.warehouse", WAREHOUSE)
        .getOrCreate()
    )


def jdbc_url():
    return "jdbc:postgresql://{}:{}/{}".format(
        _env("POSTGRES_HOST", "postgres"),
        _env("POSTGRES_PORT", "5432"),
        _env("POSTGRES_DB",   "olist_db"),
    )


def jdbc_props():
    return {
        "user":     _env("POSTGRES_USER",     "olist_admin"),
        "password": _env("POSTGRES_PASSWORD", ""),
        "driver":   "org.postgresql.Driver",
        "fetchsize": "10000",
    }
