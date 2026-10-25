"""
Gráficos específicos del modelo (coeficientes, ablación, búsqueda de hiperparámetros).
Usa sparkstats.graficos para los gráficos genéricos.
"""

from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame

from sparkstats.graficos import barras_ordenadas, curva, guardar_y_mostrar
from modelo.config import DIR_GRAFICOS


def grafico_coeficientes(modelo: PipelineModel, nombres_variables: list, carpeta: str = DIR_GRAFICOS) -> None:
    """
    Gráfico de barras horizontales de los 20 coeficientes más grandes (en magnitud).
    """
    lr_model = modelo.stages[-1]
    coefs = lr_model.coefficients.toArray()

    spark = modelo.stages[0].sparkSession if hasattr(modelo.stages[0], 'sparkSession') else None

    filas = [{"variable": nombres_variables[i], "coeficiente": float(coefs[i])}
             for i in range(min(len(nombres_variables), len(coefs)))]
    filas_top20 = sorted(filas, key=lambda x: abs(x["coeficiente"]), reverse=True)[:20]

    import plotly.graph_objects as go

    nombres = [f["variable"] for f in filas_top20]
    valores = [f["coeficiente"] for f in filas_top20]
    colores = ["#e74c3c" if v > 0 else "#3498db" for v in valores]

    fig = go.Figure(go.Bar(
        x=valores, y=nombres, orientation="h",
        marker_color=colores,
    ))
    fig.update_layout(title="Top 20 coeficientes (rojo: aumentan probabilidad tardío)", height=600)
    guardar_y_mostrar(fig, carpeta, "coeficientes_top20")


def grafico_ablacion(ablacion_df: DataFrame, carpeta: str = DIR_GRAFICOS) -> None:
    """
    Gráfico de barras del delta de log-loss por grupo de ablación.
    Positivo = empeoró al quitar ese grupo (es útil).
    """
    df_filtrado = (
        ablacion_df
        .filter(F.col("grupo") != "completo")
        .filter(F.col("delta_log_loss").isNotNull())
        .orderBy("delta_log_loss")
    )
    barras_ordenadas(
        df_filtrado, "grupo", "delta_log_loss",
        titulo="Ablación: cambio de log-loss al quitar cada grupo (train+val / val)",
        carpeta_salida=carpeta, nombre_archivo="ablacion",
        orientacion="h",
    )


def grafico_busqueda(historial: list, carpeta: str = DIR_GRAFICOS) -> None:
    """
    Log-loss de validación contra reg_param, coloreado por elastic_net_param.
    """
    import plotly.graph_objects as go
    import plotly.express as px

    reg_vals = [h["reg_param"] for h in historial]
    ll_vals  = [h["log_loss"] for h in historial]
    en_vals  = [h["elastic_net_param"] for h in historial]

    fig = go.Figure(go.Scatter(
        x=reg_vals,
        y=ll_vals,
        mode="markers",
        marker=dict(
            color=en_vals,
            colorscale="Viridis",
            colorbar=dict(title="elastic_net_param"),
            size=8,
        ),
        text=[f"min_ev={h['min_eventos']}" for h in historial],
    ))
    fig.update_layout(
        title="Log-loss de validación vs. reg_param",
        xaxis_title="reg_param (escala log)",
        yaxis_title="log-loss (validación)",
        xaxis_type="log",
    )
    guardar_y_mostrar(fig, carpeta, "busqueda_hiperparametros")
