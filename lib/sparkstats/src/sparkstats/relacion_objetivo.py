"""
Logit empírico para explorar la relación de variables con un objetivo binario.
"""

from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, StringType


def logit_empirico(
    df: DataFrame,
    col_variable: str,
    col_objetivo: str,
    n_cuantiles: int = 10,
    por_valor: bool = False,
    epsilon: float = 1e-6,
) -> DataFrame:
    """
    Logit empírico de col_objetivo en función de col_variable.

    Si por_valor=False (defecto para numéricas): agrupa por deciles de col_variable.
    Si por_valor=True (para discretas): agrupa por cada valor distinto.

    La fórmula es logit(p) = log(p / (1 - p)), con p = tasa de evento en el grupo.
    Se usa epsilon para evitar log(0).

    Parámetros
    ----------
    df : DataFrame
    col_variable : predictora
    col_objetivo : binaria (0/1)
    n_cuantiles : número de cuantiles (solo cuando por_valor=False)
    por_valor : True para agrupar por valor exacto (variables discretas)
    epsilon : valor de suavizado para evitar log(0) o log(inf)

    Devuelve un DataFrame con columnas [grupo, n, eventos, tasa, logit_empirico].
    """
    if por_valor:
        col_expr = F.coalesce(F.col(col_variable).cast(StringType()), F.lit("NA"))
        agrupado = (
            df.withColumn("__grupo", col_expr)
            .groupBy("__grupo")
            .agg(
                F.count("*").alias("n"),
                F.sum(F.col(col_objetivo).cast(DoubleType())).alias("eventos"),
            )
            .withColumnRenamed("__grupo", "grupo")
        )
    else:
        from pyspark.ml.feature import QuantileDiscretizer
        disc = QuantileDiscretizer(
            numBuckets=n_cuantiles,
            inputCol=col_variable,
            outputCol="__bucket",
            relativeError=0.01,
            handleInvalid="skip",
        )
        df_buck = disc.fit(df).transform(df)
        agrupado = (
            df_buck
            .groupBy("__bucket")
            .agg(
                F.count("*").alias("n"),
                F.sum(F.col(col_objetivo).cast(DoubleType())).alias("eventos"),
                F.mean(F.col(col_variable).cast(DoubleType())).alias("grupo"),
            )
        )

    eps = F.lit(epsilon)
    p = (F.col("eventos") / F.col("n")).cast(DoubleType())
    p_adj = F.greatest(eps, F.least(1.0 - eps, p))

    return (
        agrupado
        .withColumn("tasa", p)
        .withColumn("logit_empirico", F.log(p_adj / (1.0 - p_adj)))
        .orderBy("grupo")
    )
