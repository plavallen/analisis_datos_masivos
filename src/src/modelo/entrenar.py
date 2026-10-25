"""
Búsqueda de hiperparámetros, ablación y entrenamiento final con MLflow.
"""

from __future__ import annotations

import random
import math
import os
from typing import Optional

import mlflow
import mlflow.spark
from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from modelo.config import (
    N_COMBINACIONES_BUSQUEDA,
    SEMILLA,
    REG_PARAM_LO, REG_PARAM_HI,
    ELASTIC_NET_LO, ELASTIC_NET_HI,
    MIN_EVENTOS_OPCIONES,
    EXPERIMENTO_MLFLOW,
    DIR_MODELO,
    NOMBRE_MODELO_MLFLOW,
    ALIAS_MODELO,
)
from modelo.transformadores import construir_pipeline
from modelo.evaluacion import metricas_validacion, extraer_prob


def busqueda_hiperparametros(
    train: DataFrame,
    val: DataFrame,
    snapshots: dict,
    variables_finales: list,
    run_padre_id: Optional[str] = None,
) -> tuple[dict, list]:
    """
    Búsqueda aleatoria de N_COMBINACIONES_BUSQUEDA combinaciones de hiperparámetros.
    Registra cada combinación como run anidado en MLflow.

    Devuelve (mejor_combinacion, historial).
    """
    rng = random.Random(SEMILLA)
    combinaciones = []
    for _ in range(N_COMBINACIONES_BUSQUEDA):
        reg_param = math.exp(rng.uniform(math.log(REG_PARAM_LO), math.log(REG_PARAM_HI)))
        elastic_net = rng.uniform(ELASTIC_NET_LO, ELASTIC_NET_HI)
        min_eventos = rng.choice(MIN_EVENTOS_OPCIONES)
        combinaciones.append({
            "reg_param": reg_param,
            "elastic_net_param": elastic_net,
            "min_eventos": min_eventos,
        })

    mlflow.set_experiment(EXPERIMENTO_MLFLOW)
    historial = []

    for i, comb in enumerate(combinaciones):
        with mlflow.start_run(
            run_name=f"busqueda_{i:03d}",
            nested=(run_padre_id is not None),
        ):
            mlflow.log_params(comb)

            pipeline = construir_pipeline(
                min_eventos=comb["min_eventos"],
                reg_param=comb["reg_param"],
                elastic_net_param=comb["elastic_net_param"],
            )
            modelo = pipeline.fit(train)
            df_pred_val = modelo.transform(val)
            metricas = metricas_validacion(df_pred_val)
            mlflow.log_metrics(metricas)

            historial.append({**comb, **metricas})

    mejor = min(historial, key=lambda x: x["log_loss"])
    return mejor, historial


GRUPOS_ABLACION = {
    "promesa":    ["dias_prometidos", "margen_promesa"],
    "geografia":  ["customer_state", "seller_state_critico", "mismo_estado", "intraurbana", "distancia_critica_km"],
    "distancia":  ["distancia_critica_km"],
    "mismo_estado": ["mismo_estado"],
    "vendedor":   ["vol_seller_id_critico_90d", "tasa_seller_id_critico_90d"],
    "producto":   ["categoria_principal", "peso_medio_g", "volumen_medio_cm3", "n_items"],
    "precio_flete": ["precio_total", "flete_total"],
    "pago":       ["diff_pago", "payment_type"],
    "calendario": ["mes_del_anio", "dia_semana"],
}


def ablacion(
    mejor_comb: dict,
    train: DataFrame,
    val: DataFrame,
) -> DataFrame:
    """
    Evalúa el impacto de quitar cada grupo de variables.
    Devuelve un DataFrame con columnas [grupo, log_loss, pr_auc, delta_log_loss, delta_pr_auc].
    """
    spark = train.sparkSession

    modelo_completo = construir_pipeline(
        min_eventos=mejor_comb["min_eventos"],
        reg_param=mejor_comb["reg_param"],
        elastic_net_param=mejor_comb["elastic_net_param"],
    ).fit(train)
    met_completo = metricas_validacion(extraer_prob(modelo_completo.transform(val)))
    ll_ref = met_completo["log_loss"]
    pr_ref = met_completo["pr_auc"]

    filas = [{"grupo": "completo", "log_loss": ll_ref, "pr_auc": pr_ref,
               "delta_log_loss": 0.0, "delta_pr_auc": 0.0}]

    for grupo, cols in GRUPOS_ABLACION.items():
        train_sin = train.drop(*[c for c in cols if c in train.columns])
        val_sin   = val.drop(*[c for c in cols if c in val.columns])
        try:
            modelo_sin = construir_pipeline(
                min_eventos=mejor_comb["min_eventos"],
                reg_param=mejor_comb["reg_param"],
                elastic_net_param=mejor_comb["elastic_net_param"],
            ).fit(train_sin)
            met_sin = metricas_validacion(extraer_prob(modelo_sin.transform(val_sin)))
            filas.append({
                "grupo": grupo,
                "log_loss": met_sin["log_loss"],
                "pr_auc":   met_sin["pr_auc"],
                "delta_log_loss": met_sin["log_loss"] - ll_ref,
                "delta_pr_auc":   met_sin["pr_auc"] - pr_ref,
            })
        except Exception as e:
            filas.append({
                "grupo": grupo, "log_loss": None, "pr_auc": None,
                "delta_log_loss": None, "delta_pr_auc": None,
            })

    _schema = StructType([
        StructField("grupo",          StringType(), True),
        StructField("log_loss",       DoubleType(), True),
        StructField("pr_auc",         DoubleType(), True),
        StructField("delta_log_loss", DoubleType(), True),
        StructField("delta_pr_auc",   DoubleType(), True),
    ])
    _rows = [(f["grupo"], f["log_loss"], f["pr_auc"], f["delta_log_loss"], f["delta_pr_auc"])
             for f in filas]
    return spark.createDataFrame(_rows, schema=_schema)


def referencias(train: DataFrame, val: DataFrame) -> dict:
    """
    Evalúa dos modelos de referencia en validación:
    1. Modelo constante que predice la prevalencia de entrenamiento.
    2. Tasa histórica de la ruta (sin filtrar el futuro).
    """
    from sparkstats.evaluacion import log_loss, brier
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    import pyspark.sql.functions as F

    # 1. Modelo constante
    prevalencia = train.agg(F.mean(F.col("tarde").cast("double"))).collect()[0][0]
    val_cte = val.withColumn("prob_1", F.lit(float(prevalencia)))
    met_cte = {
        "log_loss": log_loss(val_cte, "tarde", "prob_1"),
        "brier": brier(val_cte, "tarde", "prob_1"),
    }

    # 2. Tasa histórica de la ruta
    val_ruta = val.withColumn(
        "prob_1",
        F.coalesce(F.col("tasa_seller_id_critico_90d"), F.lit(float(prevalencia))),
    )
    evaluator_pr = BinaryClassificationEvaluator(
        labelCol="tarde", rawPredictionCol="prob_1", metricName="areaUnderPR"
    )
    met_ruta = {
        "log_loss": log_loss(val_ruta, "tarde", "prob_1"),
        "brier": brier(val_ruta, "tarde", "prob_1"),
    }

    return {"constante": met_cte, "tasa_ruta": met_ruta, "prevalencia": prevalencia}


def entrenar_final(
    mejor_comb: dict,
    train: DataFrame,
    val: DataFrame,
    snapshots: dict,
    variables_finales: list,
    ablacion_df: DataFrame,
) -> tuple[PipelineModel, str]:
    """
    Entrena el modelo final con train + val y lo registra en MLflow.
    Registra el modelo con el alias 'challenger'.

    Devuelve (modelo, run_id).
    """
    mlflow.set_experiment(EXPERIMENTO_MLFLOW)
    train_val = train.union(val)

    with mlflow.start_run(run_name="modelo_final") as run:
        mlflow.log_params(mejor_comb)
        mlflow.log_params({
            "snapshot_pedidos": snapshots.get("pedidos"),
            "snapshot_envios": snapshots.get("envios"),
            "semilla": SEMILLA,
            "variables_finales": str(variables_finales),
        })

        pipeline = construir_pipeline(
            min_eventos=mejor_comb["min_eventos"],
            reg_param=mejor_comb["reg_param"],
            elastic_net_param=mejor_comb["elastic_net_param"],
        )
        modelo = pipeline.fit(train_val)

        # Guardar ablación como artefacto (collect al driver para evitar problemas de path en executors)
        import csv as _csv
        import os as _os
        _os.makedirs(DIR_MODELO, exist_ok=True)
        abl_ruta = f"{DIR_MODELO}/ablacion.csv"
        _filas = ablacion_df.collect()
        if _filas:
            with open(abl_ruta, "w", newline="") as _f:
                _w = _csv.DictWriter(_f, fieldnames=_filas[0].asDict().keys())
                _w.writeheader()
                _w.writerows([r.asDict() for r in _filas])
            mlflow.log_artifact(abl_ruta)

        mlflow.spark.log_model(
            modelo,
            artifact_path="modelo",
            registered_model_name=NOMBRE_MODELO_MLFLOW,
        )

        run_id = run.info.run_id

    # Asignar alias al modelo registrado
    client = mlflow.tracking.MlflowClient()
    versiones = client.search_model_versions(f"name='{NOMBRE_MODELO_MLFLOW}'")
    if versiones:
        ultima = max(versiones, key=lambda v: int(v.version))
        client.set_registered_model_alias(NOMBRE_MODELO_MLFLOW, ALIAS_MODELO, ultima.version)

    return modelo, run_id
