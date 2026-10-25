"""
Funciones de estadística descriptiva sobre DataFrames de PySpark.
Ninguna columna ni ruta es específica de un proyecto.
"""

from __future__ import annotations

from typing import Optional, List

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType


def resumen_numericas(
    df: DataFrame,
    columnas: List[str],
    col_objetivo: Optional[str] = None,
    umbral_casi_constante_razon: float = 19.0,
    umbral_casi_constante_pct_distintos: float = 0.10,
) -> DataFrame:
    """
    Resumen estadístico de columnas numéricas.

    Incluye: count, nulos, media, desvío, CV, p01, p25, p50, p75, p99,
    n_distintos, razón de frecuencias, % de distintos y marca de casi constante
    (razón > umbral y % distintos < umbral).

    Parámetros
    ----------
    df : DataFrame
    columnas : columnas numéricas a resumir
    col_objetivo : columna binaria (0/1) para calcular tasa de evento por columna
    umbral_casi_constante_razon : razón top-2-frecuencia / total para marcar como casi constante
    umbral_casi_constante_pct_distintos : % de distintos por debajo del cual se marca
    """
    spark = df.sparkSession
    n_total = df.count()
    filas = []
    for col in columnas:
        agg = df.agg(
            F.count(F.col(col)).alias("count"),
            F.sum(F.when(F.col(col).isNull(), 1).otherwise(0)).alias("nulos"),
            F.mean(col).alias("media"),
            F.stddev(col).alias("desvio"),
            F.percentile_approx(col, 0.01).alias("p01"),
            F.percentile_approx(col, 0.25).alias("p25"),
            F.percentile_approx(col, 0.50).alias("p50"),
            F.percentile_approx(col, 0.75).alias("p75"),
            F.percentile_approx(col, 0.99).alias("p99"),
            F.approx_count_distinct(col).alias("n_distintos"),
        ).collect()[0]

        cv = (agg["desvio"] / agg["media"]) if agg["media"] and agg["media"] != 0 else None

        # razón de frecuencias: frecuencia del valor más común / segundo más común
        top2 = (
            df.groupBy(col).count()
            .orderBy(F.desc("count"))
            .limit(2)
            .collect()
        )
        if len(top2) >= 2 and top2[1]["count"] > 0:
            razon_freq = top2[0]["count"] / top2[1]["count"]
        elif len(top2) == 1:
            razon_freq = float("inf")
        else:
            razon_freq = None

        pct_distintos = agg["n_distintos"] / n_total if n_total > 0 else None
        casi_constante = (
            razon_freq is not None
            and razon_freq > umbral_casi_constante_razon
            and pct_distintos is not None
            and pct_distintos < umbral_casi_constante_pct_distintos
        )

        fila = {
            "columna": col,
            "count": agg["count"],
            "nulos": agg["nulos"],
            "media": agg["media"],
            "desvio": agg["desvio"],
            "cv": cv,
            "p01": agg["p01"],
            "p25": agg["p25"],
            "p50": agg["p50"],
            "p75": agg["p75"],
            "p99": agg["p99"],
            "n_distintos": agg["n_distintos"],
            "razon_frecuencias": razon_freq,
            "pct_distintos": pct_distintos,
            "casi_constante": casi_constante,
        }
        filas.append(fila)

    schema_cols = [
        "columna", "count", "nulos", "media", "desvio", "cv",
        "p01", "p25", "p50", "p75", "p99", "n_distintos",
        "razon_frecuencias", "pct_distintos", "casi_constante",
    ]
    return spark.createDataFrame(filas, schema=schema_cols)


def tabla_frecuencias(
    df: DataFrame,
    columna: str,
    col_objetivo: Optional[str] = None,
    max_categorias: int = 200,
) -> DataFrame:
    """
    Tabla de frecuencias para columnas discretas o categóricas.

    Los nulos se codifican como la categoría "NA".
    Si se provee col_objetivo, agrega conteo de eventos y tasa.

    Parámetros
    ----------
    df : DataFrame
    columna : columna a tabular
    col_objetivo : columna binaria (0/1) para tasa de evento
    max_categorias : límite de filas a devolver
    """
    col_expr = F.coalesce(F.col(columna).cast(StringType()), F.lit("NA")).alias(columna)
    agrupado = df.withColumn(columna, col_expr)

    if col_objetivo:
        resultado = (
            agrupado.groupBy(columna)
            .agg(
                F.count("*").alias("n"),
                F.sum(F.col(col_objetivo).cast("double")).alias("eventos"),
            )
            .withColumn("tasa", F.col("eventos") / F.col("n"))
            .orderBy(F.desc("eventos"))
        )
    else:
        resultado = (
            agrupado.groupBy(columna)
            .agg(F.count("*").alias("n"))
            .orderBy(F.desc("n"))
        )

    return resultado.limit(max_categorias)


def entropia_normalizada(df: DataFrame, columna: str) -> float:
    """
    Entropía de Shannon normalizada (0 = constante, 1 = distribución uniforme).

    H_norm = -sum(p * log2(p)) / log2(k), donde k = número de categorías.

    Referencia: Cover & Thomas, "Elements of Information Theory", 2006.
    """
    from math import log2

    col_expr = F.coalesce(F.col(columna).cast(StringType()), F.lit("NA"))
    total = df.count()
    if total == 0:
        return 0.0

    frecuencias = (
        df.groupBy(col_expr.alias("_cat"))
        .count()
        .withColumn("p", F.col("count") / total)
        .withColumn("contrib", F.when(F.col("p") > 0, -F.col("p") * F.log2(F.col("p"))).otherwise(0.0))
        .agg(F.sum("contrib").alias("h"), F.count("*").alias("k"))
        .collect()[0]
    )
    h = frecuencias["h"] or 0.0
    k = frecuencias["k"] or 1
    return h / log2(k) if k > 1 else 0.0


def cardinalidad(df: DataFrame, columnas: List[str]) -> DataFrame:
    """
    Cardinalidad aproximada de cada columna (incluye "NA" por nulos).

    Devuelve un DataFrame con columnas [columna, n_distintos, n_nulos].
    """
    spark = df.sparkSession
    filas = []
    for col in columnas:
        agg = df.agg(
            F.approx_count_distinct(col).alias("n_distintos"),
            F.sum(F.when(F.col(col).isNull(), 1).otherwise(0)).alias("n_nulos"),
        ).collect()[0]
        filas.append({"columna": col, "n_distintos": agg["n_distintos"], "n_nulos": agg["n_nulos"]})
    return spark.createDataFrame(filas, schema=["columna", "n_distintos", "n_nulos"])


def categorias_raras(
    df: DataFrame,
    columna: str,
    col_objetivo: str,
    min_eventos: int = 20,
) -> DataFrame:
    """
    Devuelve las categorías con menos de min_eventos en col_objetivo.

    Parámetros
    ----------
    df : DataFrame
    columna : columna categórica
    col_objetivo : columna binaria (0/1)
    min_eventos : umbral mínimo de eventos para no ser "rara"
    """
    col_expr = F.coalesce(F.col(columna).cast(StringType()), F.lit("NA"))
    return (
        df.withColumn(columna, col_expr)
        .groupBy(columna)
        .agg(
            F.count("*").alias("n"),
            F.sum(F.col(col_objetivo).cast("double")).alias("eventos"),
        )
        .filter(F.col("eventos") < min_eventos)
        .orderBy("eventos")
    )


def nulos_por_conjunto(
    dfs: dict[str, DataFrame],
    columnas: List[str],
) -> DataFrame:
    """
    Compara el porcentaje de nulos de cada columna en varios DataFrames.

    Parámetros
    ----------
    dfs : dict nombre -> DataFrame
    columnas : columnas a evaluar (deben existir en todos los DataFrames)

    Devuelve un DataFrame con columnas [columna, <nombre1>, <nombre2>, ...].
    """
    spark = list(dfs.values())[0].sparkSession
    filas = []
    for col in columnas:
        fila: dict = {"columna": col}
        for nombre, df in dfs.items():
            total = df.count()
            nulos = df.filter(F.col(col).isNull()).count()
            fila[nombre] = round(nulos / total * 100, 2) if total > 0 else None
        filas.append(fila)
    schema = ["columna"] + list(dfs.keys())
    return spark.createDataFrame(filas, schema=schema)
