"""
riesgo_pedidos.py
=================
Calcula el riesgo de atraso de pedidos en curso al corte simulado y
escribe la tabla gold Iceberg `riesgo_pedidos`.

Columnas de salida:
  order_id, seller_id, ruta, categoria, fecha_prometida,
  dias_hasta_promesa, puntaje, percentil_puntaje, nivel_riesgo,
  riesgo_relativo, riesgo_num, version_modelo, fecha_corte

Uso desde el contenedor jupyter:
  python3 /home/notebooks/../../../scripts/riesgo_pedidos.py
O bien desde el host:
  docker compose -f etapa2/docker-compose.yml exec jupyter \
    python3 /home/notebooks/etapa3/../../../scripts/riesgo_pedidos.py
"""

import os, sys
import numpy as np
import pandas as pd
from scipy.stats import percentileofscore

import mlflow
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.functions import vector_to_array

# ── Rutas para importar config desde el notebook (si se ejecuta desde allí) ──
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

# sparkstats y modelo deben estar en el path para cargar el PipelineModel
for _p in ["/repo/etapa2/lib/sparkstats/src", "/repo/etapa2/src"]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from config import (
    CUTOFF_DATE, FECHA_FIN_TRAIN, FECHA_FIN_VAL,
    MODEL_NAME, MODEL_VERSION, PERCENTIL_ALTO, PERCENTIL_MEDIO,
)

TABLA_GOLD      = "lake.gold.riesgo_pedidos"
CHECKPOINT_PATH = "s3a://olist/checkpoints/df_features"

# ── SparkSession ──────────────────────────────────────────────────────────────
def get_spark():
    jars = ",".join([
        "/opt/spark/extra-jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar",
        "/opt/spark/extra-jars/hadoop-aws-3.4.2.jar",
        "/opt/spark/extra-jars/aws-sdk-bundle-v2-2.29.52.jar",
    ])
    return (
        SparkSession.builder
        .appName("riesgo-pedidos")
        .master("spark://spark-master:7077")
        .config("spark.jars", jars)
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


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    mlflow.set_tracking_uri("http://mlflow:5001")

    # ── 1. Cargar modelo ──────────────────────────────────────────────────────
    # El modelo se carga desde S3A directamente (el artefacto MLflow no tiene
    # los datos: solo success markers). El reentrenamiento lo guardó en:
    #   s3a://olist/models/olist_tarde_logistica_v1
    from pyspark.ml import PipelineModel
    model_path = "s3a://olist/models/olist_tarde_logistica_v1"
    print(f"Cargando modelo desde {model_path} ...")
    model = PipelineModel.load(model_path)
    version_str = f"{MODEL_NAME}/v{MODEL_VERSION}"

    # ── 2. Cargar checkpoint ──────────────────────────────────────────────────
    print("Cargando checkpoint features ...")
    df = spark.read.parquet(CHECKPOINT_PATH)

    # ── 3. Distribución de puntajes en el set de validación ──────────────────
    print("Calculando distribución de validación ...")
    df_val = df.filter(
        (F.col("fecha_compra") > F.lit(FECHA_FIN_TRAIN)) &
        (F.col("fecha_compra") <= F.lit(FECHA_FIN_VAL)) &
        F.col("en_periodo_util")
    )
    pred_val = model.transform(df_val)
    val_scores = (
        pred_val
        .select(vector_to_array(F.col("probability")).getItem(1).alias("s"))
        .toPandas()["s"]
        .values
    )
    val_scores_sorted = np.sort(val_scores)
    mean_val = float(val_scores_sorted.mean())
    print(f"  Val: {len(val_scores_sorted):,} pedidos | score medio={mean_val:.4f}")

    # ── 4. Pedidos en curso al corte ──────────────────────────────────────────
    print(f"Filtrando pedidos en curso al {CUTOFF_DATE} ...")
    df_en_curso = df.filter(
        (F.to_date(F.col("order_purchase_timestamp")) < F.lit(CUTOFF_DATE)) &
        (
            F.col("order_delivered_customer_date").isNull() |
            (F.to_date(F.col("order_delivered_customer_date")) > F.lit(CUTOFF_DATE))
        ) &
        (~F.col("order_status").isin("canceled", "unavailable"))
    )
    n_en_curso = df_en_curso.count()
    print(f"  Pedidos en curso: {n_en_curso:,}")

    # ── 5. Scoring ────────────────────────────────────────────────────────────
    print("Scoring con el modelo ...")
    pred = model.transform(df_en_curso)

    cols_salida = [
        "order_id", "seller_id_critico", "seller_state_critico", "customer_state",
        "categoria_principal", "order_estimated_delivery_date",
    ]
    pred_pd = (
        pred
        .select(
            *cols_salida,
            vector_to_array(F.col("probability")).getItem(1).alias("puntaje"),
        )
        .toPandas()
    )

    # ── 6. Métricas derivadas en Pandas ───────────────────────────────────────
    # Percentil vs distribución de validación
    pred_pd["percentil_puntaje"] = pred_pd["puntaje"].apply(
        lambda s: round(percentileofscore(val_scores_sorted, s, kind="rank"), 1)
    )

    # Días hasta la promesa (antes del nivel de riesgo, que usa fecha_prometida)
    cutoff_ts = pd.Timestamp(CUTOFF_DATE)
    pred_pd["fecha_prometida"] = pd.to_datetime(pred_pd["order_estimated_delivery_date"]).dt.date
    pred_pd["dias_hasta_promesa"] = (
        pd.to_datetime(pred_pd["order_estimated_delivery_date"]) - cutoff_ts
    ).dt.days

    # Nivel de riesgo (Vencido: fecha prometida anterior al corte)
    cutoff_date_obj = pd.Timestamp(CUTOFF_DATE).date()

    def nivel(row):
        if row["fecha_prometida"] < cutoff_date_obj:
            return "Vencido"
        p = row["percentil_puntaje"]
        if p >= PERCENTIL_ALTO:  return "Alto"
        if p >= PERCENTIL_MEDIO: return "Medio"
        return "Bajo"

    pred_pd["nivel_riesgo"] = pred_pd.apply(nivel, axis=1)

    # Riesgo relativo (respecto del puntaje medio de la distribución de val)
    pred_pd["riesgo_relativo"] = (pred_pd["puntaje"] / mean_val).round(2)

    # Columna numérica auxiliar para ordenamiento y formato condicional
    pred_pd["riesgo_num"] = pred_pd["nivel_riesgo"].map(
        {"Vencido": 4, "Alto": 3, "Medio": 2, "Bajo": 1}
    )

    # Columnas de presentación
    pred_pd["ruta"]           = pred_pd["seller_state_critico"] + " → " + pred_pd["customer_state"]
    pred_pd["seller_id"]      = pred_pd["seller_id_critico"].str[:8]
    pred_pd["categoria"]      = pred_pd["categoria_principal"]
    pred_pd["version_modelo"] = version_str
    pred_pd["fecha_corte"]    = CUTOFF_DATE

    # Redondeos
    pred_pd["puntaje"]  = pred_pd["puntaje"].round(4)

    # Estadísticas resumen
    dist = pred_pd["nivel_riesgo"].value_counts()
    print(f"  vencido={dist.get('Vencido',0):,}  alto={dist.get('Alto',0):,}  "
          f"medio={dist.get('Medio',0):,}  bajo={dist.get('Bajo',0):,}")

    # ── 7. Escribir a Iceberg ─────────────────────────────────────────────────
    final_cols = [
        "order_id", "seller_id", "ruta", "categoria",
        "fecha_prometida", "dias_hasta_promesa",
        "puntaje", "percentil_puntaje", "nivel_riesgo", "riesgo_num",
        "riesgo_relativo", "version_modelo", "fecha_corte",
    ]
    print(f"Escribiendo {len(pred_pd):,} filas → {TABLA_GOLD} ...")
    df_spark = spark.createDataFrame(pred_pd[final_cols])
    df_spark.writeTo(TABLA_GOLD).createOrReplace()
    print("OK")

    # ── 8. Verificación rápida ────────────────────────────────────────────────
    df_check = spark.sql(f"SELECT nivel_riesgo, COUNT(*) AS n FROM {TABLA_GOLD} GROUP BY 1")
    df_check.show()

    spark.stop()
    return pred_pd


if __name__ == "__main__":
    main()
