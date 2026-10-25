"""
Intervalos de confianza para tasas.
"""

from __future__ import annotations

from typing import List, Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, StringType


def wilson(
    df: DataFrame,
    col_exitos: str,
    col_total: str,
    nivel_confianza: float = 0.95,
) -> DataFrame:
    """
    Intervalo de confianza de Wilson para una proporción.

    Fórmula: (p̂ + z²/2n ± z·sqrt(p̂(1−p̂)/n + z²/4n²)) / (1 + z²/n)

    Referencia: Wilson (1927), "Probable inference, the law of succession,
                and statistical inference", JASA.

    Parámetros
    ----------
    df : DataFrame con columnas de éxitos y total
    col_exitos : columna con el conteo de éxitos (entero o double)
    col_total : columna con el total de intentos
    nivel_confianza : nivel de confianza (por defecto 0.95)

    Devuelve el mismo DataFrame con columnas adicionales:
        tasa, wilson_lo, wilson_hi
    """
    from scipy.stats import norm
    z = float(norm.ppf((1 + nivel_confianza) / 2))

    z2 = z ** 2
    p = F.col(col_exitos).cast(DoubleType()) / F.col(col_total).cast(DoubleType())
    n = F.col(col_total).cast(DoubleType())

    centro = (p + z2 / (2 * n)) / (1 + z2 / n)
    radio = (F.sqrt(p * (1 - p) / n + z2 / (4 * n * n))) * z / (1 + z2 / n)

    return (
        df.withColumn("tasa", p)
        .withColumn("wilson_lo", F.greatest(F.lit(0.0), centro - radio))
        .withColumn("wilson_hi", F.least(F.lit(1.0), centro + radio))
    )


def tasa_por_grupo(
    df: DataFrame,
    col_grupo: str,
    col_objetivo: str,
    nivel_confianza: float = 0.95,
) -> DataFrame:
    """
    Tasa de evento con intervalo de Wilson para cada valor de col_grupo.

    Los nulos en col_grupo se agrupan bajo la categoría "NA".

    Parámetros
    ----------
    df : DataFrame
    col_grupo : columna de agrupamiento (categórica o discreta)
    col_objetivo : columna binaria (0/1)
    nivel_confianza : nivel de confianza del intervalo

    Devuelve un DataFrame con columnas:
        [col_grupo, n, eventos, tasa, wilson_lo, wilson_hi]
    ordenado por tasa descendente.
    """
    grupo_expr = F.coalesce(F.col(col_grupo).cast(StringType()), F.lit("NA"))

    agrupado = (
        df.withColumn(col_grupo, grupo_expr)
        .groupBy(col_grupo)
        .agg(
            F.count("*").alias("n"),
            F.sum(F.col(col_objetivo).cast(DoubleType())).alias("eventos"),
        )
    )
    return wilson(agrupado, "eventos", "n", nivel_confianza=nivel_confianza).orderBy(
        F.desc("tasa")
    )
