"""
Evaluación completa del modelo: métricas, calibración, lift, umbral por costo.
"""

from __future__ import annotations

from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.ml.evaluation import BinaryClassificationEvaluator

from sparkstats.evaluacion import (
    log_loss,
    brier,
    tabla_calibracion,
    calibracion_global,
    pendiente_calibracion,
    lift_por_decil,
    puntos_roc_pr,
    metricas_umbral,
)
from sparkstats.graficos import curva, barras_ordenadas, heatmap_matriz, guardar_y_mostrar
from modelo.config import UMBRAL_COSTO, C_INTERVENCION, C_TARDE, DIR_GRAFICOS


def extraer_prob(df_pred: DataFrame, col_prob_out: str = "prob_1") -> DataFrame:
    """Extrae la probabilidad de la clase 1 desde la columna 'probability' del modelo."""
    from pyspark.ml.functions import vector_to_array
    return df_pred.withColumn(col_prob_out, vector_to_array(F.col("probability"))[1])


def metricas_validacion(df_pred: DataFrame) -> dict:
    """
    Calcula métricas de validación: log-loss, Brier, PR-AUC, ROC-AUC.
    Devuelve un diccionario con las cuatro métricas.
    """
    df_pred = extraer_prob(df_pred).withColumn("tarde", F.col("tarde").cast("int"))
    evaluator_pr = BinaryClassificationEvaluator(
        labelCol="tarde", rawPredictionCol="rawPrediction", metricName="areaUnderPR"
    )
    evaluator_roc = BinaryClassificationEvaluator(
        labelCol="tarde", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    )
    return {
        "log_loss": log_loss(df_pred, "tarde", "prob_1"),
        "brier":    brier(df_pred, "tarde", "prob_1"),
        "pr_auc":   evaluator_pr.evaluate(df_pred),
        "roc_auc":  evaluator_roc.evaluate(df_pred),
    }


def evaluacion_completa(
    modelo: PipelineModel,
    df_test: DataFrame,
    carpeta: str = DIR_GRAFICOS,
) -> dict:
    """
    Evaluación final en el conjunto de prueba. Solo se llama una vez.

    Genera todos los gráficos especificados en el paso 12 y devuelve
    un diccionario con todas las métricas.
    """
    df_pred = extraer_prob(modelo.transform(df_test))
    metricas = metricas_validacion(df_pred)

    # Calibración
    prob_media, tasa_obs = calibracion_global(df_pred, "tarde", "prob_1")
    pendiente = pendiente_calibracion(df_pred, "tarde", "prob_1")
    metricas["calibracion_global_predicho"] = prob_media
    metricas["calibracion_global_observado"] = tasa_obs
    metricas["pendiente_calibracion"] = pendiente

    # Tabla de calibración y gráfico
    cal_tabla = tabla_calibracion(df_pred, "tarde", "prob_1")
    fig_cal = curva(
        cal_tabla, "prob_media", "tasa_observada",
        titulo="Curva de calibración por deciles",
        linea_referencia=[0, 1, 0, 1],
        carpeta_salida=carpeta, nombre_archivo="calibracion",
    )

    # ROC y PR
    puntos = puntos_roc_pr(df_pred, "tarde", "prob_1")
    fig_roc = curva(
        puntos, "fpr", "tpr",
        titulo="Curva ROC",
        linea_referencia=[0, 1, 0, 1],
        carpeta_salida=carpeta, nombre_archivo="roc",
    )

    # Calcular PR-AUC como área bajo la curva PR (prevalencia como línea base)
    prevalencia = df_test.agg(F.mean(F.col("tarde").cast("double"))).collect()[0][0]
    fig_pr = curva(
        puntos, "recall", "precision",
        titulo=f"Curva PR (prevalencia = {prevalencia:.3f})",
        linea_referencia=[0, 1, prevalencia, prevalencia],
        carpeta_salida=carpeta, nombre_archivo="pr",
    )
    metricas["prevalencia"] = prevalencia

    # Lift por decil
    lift_tabla = lift_por_decil(df_pred, "tarde", "prob_1")
    barras_ordenadas(
        lift_tabla, "decil", "lift",
        titulo="Lift por decil del puntaje predicho",
        carpeta_salida=carpeta, nombre_archivo="lift",
        orientacion="v",
    )

    # Histograma de puntajes por clase
    _histograma_puntajes(df_pred, "prob_1", "tarde", carpeta)

    # Umbral por costo
    umbral = UMBRAL_COSTO
    met_umbral = metricas_umbral(df_pred, "tarde", "prob_1", umbral).collect()[0]
    metricas["umbral"] = umbral
    metricas["precision_umbral"] = met_umbral["precision"]
    metricas["recall_umbral"] = met_umbral["recall"]
    metricas["n_positivos_umbral"] = met_umbral["n_positivos"]

    # Sensibilidad a tres cocientes de costo
    metricas["sensibilidad_costo"] = _tabla_sensibilidad(df_pred, carpeta)

    return metricas


def _histograma_puntajes(df: DataFrame, col_prob: str, col_verdad: str, carpeta: str) -> None:
    """Histograma de puntajes separado por clase."""
    import plotly.graph_objects as go

    for etiqueta in [0, 1]:
        cuantiles = df.filter(F.col(col_verdad) == etiqueta).approxQuantile(
            col_prob, [i / 20 for i in range(21)], 0.01
        )
        conteos = []
        for i in range(len(cuantiles) - 1):
            n = df.filter(
                (F.col(col_verdad) == etiqueta)
                & (F.col(col_prob) >= cuantiles[i])
                & (F.col(col_prob) < cuantiles[i + 1])
            ).count()
            conteos.append(n)

        fig = go.Figure(go.Bar(
            x=[(cuantiles[i] + cuantiles[i + 1]) / 2 for i in range(len(cuantiles) - 1)],
            y=conteos,
            name=f"clase {etiqueta}",
        ))
        fig.update_layout(title=f"Distribución de puntajes: clase {etiqueta}")
        guardar_y_mostrar(fig, carpeta, f"hist_puntajes_clase{etiqueta}")


def _tabla_sensibilidad(df: DataFrame, carpeta: str) -> list:
    """Tabla de sensibilidad para tres cocientes de costo."""
    resultados = []
    for cociente in [0.1, 0.2, 0.5]:
        met = metricas_umbral(df, "tarde", "prob_1", cociente).collect()[0]
        resultados.append({
            "cociente_costo": cociente,
            "umbral": cociente,
            "precision": met["precision"],
            "recall": met["recall"],
            "n_positivos": met["n_positivos"],
        })
    return resultados


def estabilidad_en_prueba(
    df_test: DataFrame,
    modelo: PipelineModel,
    carpeta: str = DIR_GRAFICOS,
) -> tuple[DataFrame, DataFrame]:
    """
    Calibración global y PR-AUC por mes y por región del cliente.
    Señala mayo de 2018 (huelga de camioneros).
    """
    from pyspark.ml.evaluation import BinaryClassificationEvaluator

    df_pred = extraer_prob(modelo.transform(df_test))

    evaluator_pr = BinaryClassificationEvaluator(
        labelCol="tarde", rawPredictionCol="rawPrediction", metricName="areaUnderPR"
    )

    # Por mes
    meses = [
        r["mes_compra"]
        for r in df_pred.select("mes_compra").distinct().orderBy("mes_compra").collect()
    ]
    filas_mes = []
    for mes in meses:
        df_mes = df_pred.filter(F.col("mes_compra") == mes)
        pm, to = calibracion_global(df_mes, "tarde", "prob_1")
        pr = evaluator_pr.evaluate(df_mes) if df_mes.count() > 50 else None
        huelga = mes == "2018-05"
        filas_mes.append({
            "mes": mes, "prob_media": pm, "tasa_obs": to,
            "pr_auc": pr, "huelga_camioneros": huelga,
        })

    spark = df_pred.sparkSession
    por_mes = spark.createDataFrame(filas_mes)

    # Por región
    regiones = [r["customer_region"] for r in df_pred.select("customer_region").distinct().collect()]
    filas_reg = []
    for region in regiones:
        df_reg = df_pred.filter(F.col("customer_region") == region)
        pm, to = calibracion_global(df_reg, "tarde", "prob_1")
        pr = evaluator_pr.evaluate(df_reg) if df_reg.count() > 50 else None
        filas_reg.append({"region": str(region), "prob_media": pm, "tasa_obs": to, "pr_auc": pr})

    por_region = spark.createDataFrame(filas_reg)

    return por_mes, por_region
