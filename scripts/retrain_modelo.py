"""
retrain_modelo.py
=================
Reentrena el modelo olist_tarde_logistica con los hiperparámetros finales
y lo guarda en S3A en una ruta accesible para PipelineModel.load().

El modelo queda disponible en:  s3a://olist/models/olist_tarde_logistica_v1

El checkpoint de features (ya computado) se usa como fuente de datos,
por lo que el entrenamiento es solo el fitting del pipeline (~2-5 min).

Uso:
    docker compose exec jupyter \
        python3 /repo/scripts/retrain_modelo.py
"""

import sys, os
sys.path.insert(0, "/repo/src")
sys.path.insert(0, "/repo/lib/sparkstats/src")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import mlflow

CHECKPOINT_PATH = "s3a://olist/checkpoints/df_features"
MODEL_S3A_PATH  = "s3a://olist/models/olist_tarde_logistica_v1"

# Hiperparámetros finales del notebook 01
REG_PARAM    = 0.0361166278214725
ELASTIC_NET  = 0.025010755222666936
MIN_EVENTOS  = 30
SEED         = 42

FECHA_FIN_TRAIN = "2017-12-31"

JARS = ",".join([
    "/opt/spark/extra-jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar",
    "/opt/spark/extra-jars/hadoop-aws-3.4.2.jar",
    "/opt/spark/extra-jars/aws-sdk-bundle-v2-2.29.52.jar",
])

spark = (
    SparkSession.builder
    .appName("retrain-olist-tarde")
    .master("spark://spark-master:7077")
    .config("spark.jars", JARS)
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lake.type", "hadoop")
    .config("spark.sql.catalog.lake.warehouse", "s3a://olist/warehouse")
    .config("spark.hadoop.fs.s3a.endpoint",
            os.environ.get("RUSTFS_ENDPOINT", "http://rustfs:9000"))
    .config("spark.hadoop.fs.s3a.access.key",  os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key",  os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.driver.extraClassPath",
            os.environ.get("SPARK_CLASSPATH", ""))
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

from modelo.transformadores import construir_pipeline

# ── Datos ─────────────────────────────────────────────────────────────────────
print("Cargando checkpoint ...")
df = spark.read.parquet(CHECKPOINT_PATH)
train = (
    df
    .filter(F.col("en_periodo_util") & (F.col("fecha_compra") <= FECHA_FIN_TRAIN))
    .withColumn("tarde", F.col("tarde").cast("int"))
)
n_train = train.count()
print(f"Train: {n_train:,} filas")

# ── Entrenamiento ─────────────────────────────────────────────────────────────
print(f"Entrenando pipeline (reg={REG_PARAM:.4f}, elastic={ELASTIC_NET:.4f}, min_ev={MIN_EVENTOS}) ...")
pipeline = construir_pipeline(
    min_eventos=MIN_EVENTOS,
    reg_param=REG_PARAM,
    elastic_net_param=ELASTIC_NET,
)
model = pipeline.fit(train)
print("Entrenamiento completado.")

# ── Guardar en S3A ────────────────────────────────────────────────────────────
print(f"Guardando en {MODEL_S3A_PATH} ...")
model.write().overwrite().save(MODEL_S3A_PATH)
print("Guardado OK.")

# ── Verificar carga ───────────────────────────────────────────────────────────
from pyspark.ml import PipelineModel
model_check = PipelineModel.load(MODEL_S3A_PATH)
test_pred = model_check.transform(train.limit(10))
assert test_pred.filter(F.col("probability").isNotNull()).count() > 0
print("Verificación: carga y predicción OK.")

spark.stop()
print("Listo. Modelo en:", MODEL_S3A_PATH)
