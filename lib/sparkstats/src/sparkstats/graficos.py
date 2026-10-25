"""
Funciones de visualización sobre DataFrames de PySpark.
Gráficos desde datos agregados, guardados como PNG con kaleido.
"""

from __future__ import annotations

import os
from typing import Optional, List

from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def guardar_y_mostrar(fig, carpeta_salida: str, nombre_archivo: str, scale: int = 2) -> str:
    """
    Guarda una figura Plotly como PNG con kaleido y la devuelve para mostrar en notebook.

    Parámetros
    ----------
    fig : figura de Plotly
    carpeta_salida : ruta de directorio donde guardar
    nombre_archivo : nombre del archivo sin extensión
    scale : factor de escala para kaleido (2 = resolución 2x)

    Devuelve la ruta completa del archivo guardado.
    """
    os.makedirs(carpeta_salida, exist_ok=True)
    ruta = os.path.join(carpeta_salida, f"{nombre_archivo}.png")
    fig.write_image(ruta, scale=scale)
    return ruta


def heatmap_matriz(
    df: DataFrame,
    col_a: str,
    col_b: str,
    col_valor: str,
    titulo: str = "",
    carpeta_salida: Optional[str] = None,
    nombre_archivo: str = "heatmap",
) -> object:
    """
    Mapa de calor a partir de un DataFrame con pares (col_a, col_b, col_valor).
    Devuelve la figura de Plotly y la guarda si se provee carpeta_salida.
    """
    import plotly.graph_objects as go

    filas = df.collect()
    etiquetas_a = sorted(set(r[col_a] for r in filas))
    etiquetas_b = sorted(set(r[col_b] for r in filas))
    idx_a = {v: i for i, v in enumerate(etiquetas_a)}
    idx_b = {v: i for i, v in enumerate(etiquetas_b)}

    import numpy as np
    matriz = np.full((len(etiquetas_a), len(etiquetas_b)), float("nan"))
    for r in filas:
        i, j = idx_a[r[col_a]], idx_b[r[col_b]]
        matriz[i][j] = r[col_valor]

    fig = go.Figure(
        data=go.Heatmap(
            z=matriz,
            x=etiquetas_b,
            y=etiquetas_a,
            colorscale="RdBu_r",
            zmid=0,
            text=[[f"{v:.2f}" if not (v != v) else "" for v in row] for row in matriz],
            texttemplate="%{text}",
        )
    )
    fig.update_layout(title=titulo, width=800, height=700)

    if carpeta_salida:
        guardar_y_mostrar(fig, carpeta_salida, nombre_archivo)

    return fig


def barras_ordenadas(
    df: DataFrame,
    col_categoria: str,
    col_valor: str,
    titulo: str = "",
    n_max: int = 20,
    carpeta_salida: Optional[str] = None,
    nombre_archivo: str = "barras",
    orientacion: str = "h",
) -> object:
    """
    Gráfico de barras horizontales u ordenadas a partir de un DataFrame.

    Parámetros
    ----------
    df : DataFrame con col_categoria y col_valor
    col_categoria : eje de categorías
    col_valor : eje de valores
    titulo : título del gráfico
    n_max : máximo de barras a mostrar
    carpeta_salida : carpeta de salida para guardar PNG
    nombre_archivo : nombre del archivo PNG (sin extensión)
    orientacion : "h" para horizontal, "v" para vertical
    """
    filas = df.orderBy(F.col(col_valor)).limit(n_max).collect()
    categorias = [str(r[col_categoria]) for r in filas]
    valores = [float(r[col_valor]) for r in filas]

    import plotly.graph_objects as go

    if orientacion == "h":
        fig = go.Figure(go.Bar(x=valores, y=categorias, orientation="h"))
    else:
        fig = go.Figure(go.Bar(x=categorias, y=valores))

    fig.update_layout(title=titulo, height=max(400, 30 * len(categorias)))

    if carpeta_salida:
        guardar_y_mostrar(fig, carpeta_salida, nombre_archivo)

    return fig


def curva(
    df: DataFrame,
    col_x: str,
    col_y: str,
    titulo: str = "",
    linea_referencia: Optional[List[float]] = None,
    carpeta_salida: Optional[str] = None,
    nombre_archivo: str = "curva",
) -> object:
    """
    Gráfico de línea simple (curva ROC, PR, calibración, etc.).

    Parámetros
    ----------
    df : DataFrame con col_x y col_y, ordenado
    col_x, col_y : columnas de los ejes
    titulo : título del gráfico
    linea_referencia : si se provee [x0, x1, y0, y1], dibuja una línea de referencia diagonal
    carpeta_salida : carpeta de salida
    nombre_archivo : nombre del archivo PNG (sin extensión)
    """
    import plotly.graph_objects as go

    filas = df.orderBy(col_x).collect()
    x_vals = [float(r[col_x]) for r in filas]
    y_vals = [float(r[col_y]) for r in filas]

    traces = [go.Scatter(x=x_vals, y=y_vals, mode="lines+markers", name=col_y)]

    if linea_referencia:
        x0, x1, y0, y1 = linea_referencia
        traces.append(go.Scatter(
            x=[x0, x1], y=[y0, y1], mode="lines",
            line=dict(dash="dash", color="gray"), name="referencia",
        ))

    fig = go.Figure(data=traces)
    fig.update_layout(title=titulo, xaxis_title=col_x, yaxis_title=col_y)

    if carpeta_salida:
        guardar_y_mostrar(fig, carpeta_salida, nombre_archivo)

    return fig


def box_robusto(
    df: DataFrame,
    columnas: List[str],
    titulo: str = "",
    carpeta_salida: Optional[str] = None,
    nombre_archivo: str = "box_robusto",
) -> object:
    """
    Box plot en escala robusta: (x - mediana) / IQR.

    Agrega los cuantiles en Spark (p25, p50, p75) y construye el box en Plotly.

    Parámetros
    ----------
    df : DataFrame
    columnas : lista de columnas numéricas
    titulo : título del gráfico
    carpeta_salida : carpeta de salida
    nombre_archivo : nombre del archivo PNG (sin extensión)
    """
    import plotly.graph_objects as go

    traces = []
    for col in columnas:
        p25, p50, p75 = df.approxQuantile(col, [0.25, 0.5, 0.75], 0.01)
        iqr = p75 - p25 if p75 > p25 else 1.0

        stats = df.agg(
            F.percentile_approx(col, 0.05).alias("p05"),
            F.percentile_approx(col, 0.95).alias("p95"),
        ).collect()[0]

        q1_r = (p25 - p50) / iqr
        q3_r = (p75 - p50) / iqr
        median_r = 0.0
        lo_r = (stats["p05"] - p50) / iqr
        hi_r = (stats["p95"] - p50) / iqr

        traces.append(go.Box(
            name=col,
            q1=[q1_r], median=[median_r], q3=[q3_r],
            lowerfence=[lo_r], upperfence=[hi_r],
            boxpoints=False,
        ))

    fig = go.Figure(data=traces)
    fig.update_layout(title=titulo, yaxis_title="(x - mediana) / IQR")

    if carpeta_salida:
        guardar_y_mostrar(fig, carpeta_salida, nombre_archivo)

    return fig
