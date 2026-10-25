"""
Medidas de asociación entre variables: V de Cramér, correlaciones y VIF.
Todo sobre PySpark DataFrames.
"""

from __future__ import annotations

from typing import List, Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType


def cramer_v(
    df: DataFrame,
    col_a: str,
    col_b: str,
    correccion_bergsma: bool = True,
) -> float:
    """
    V de Cramér entre dos columnas categóricas.

    Con correccion_bergsma=True aplica la corrección de Bergsma (2013) para
    reducir el sesgo en tablas con pocas observaciones.

    Referencia: Bergsma (2013), "A bias-correction for Cramér's V and Tschuprow's T".

    Devuelve un escalar entre 0 y 1.
    """
    cat_a = F.coalesce(F.col(col_a).cast(StringType()), F.lit("NA"))
    cat_b = F.coalesce(F.col(col_b).cast(StringType()), F.lit("NA"))

    tabla = (
        df.withColumn("__a", cat_a).withColumn("__b", cat_b)
        .groupBy("__a", "__b")
        .count()
    )
    n = df.count()
    if n == 0:
        return 0.0

    chi2_row = (
        tabla
        .withColumn("n", F.lit(n))
        .withColumn("p_a", F.sum("count").over(__window_col("__a")) / F.col("n"))
        .withColumn("p_b", F.sum("count").over(__window_col("__b")) / F.col("n"))
        .withColumn("esperado", F.col("p_a") * F.col("p_b") * F.col("n"))
        .withColumn("chi2_contrib", (F.col("count") - F.col("esperado")) ** 2 / F.col("esperado"))
        .agg(
            F.sum("chi2_contrib").alias("chi2"),
            F.approx_count_distinct("__a").alias("r"),
            F.approx_count_distinct("__b").alias("k"),
        )
        .collect()[0]
    )

    chi2 = chi2_row["chi2"] or 0.0
    r = chi2_row["r"]
    k = chi2_row["k"]

    phi2 = chi2 / n
    if correccion_bergsma:
        phi2_corr = max(0.0, phi2 - (r - 1) * (k - 1) / (n - 1))
        r_corr = r - (r - 1) ** 2 / (n - 1)
        k_corr = k - (k - 1) ** 2 / (n - 1)
        denom = min(r_corr - 1, k_corr - 1)
        return (phi2_corr / denom) ** 0.5 if denom > 0 else 0.0
    else:
        denom = min(r - 1, k - 1)
        return (phi2 / denom) ** 0.5 if denom > 0 else 0.0


def __window_col(col_name: str):
    from pyspark.sql.window import Window
    return Window.partitionBy(col_name)


def matriz_cramer(
    df: DataFrame,
    columnas: List[str],
    correccion_bergsma: bool = True,
) -> DataFrame:
    """
    Matriz de V de Cramér entre todas las columnas categóricas.

    Devuelve un DataFrame triangular inferior (col_a, col_b, cramer_v).
    """
    spark = df.sparkSession
    filas = []
    for i, col_a in enumerate(columnas):
        for col_b in columnas[i + 1:]:
            v = cramer_v(df, col_a, col_b, correccion_bergsma=correccion_bergsma)
            filas.append({"col_a": col_a, "col_b": col_b, "cramer_v": float(v)})
    return spark.createDataFrame(filas, schema=["col_a", "col_b", "cramer_v"])


def matriz_correlacion(
    df: DataFrame,
    columnas: List[str],
    metodo: str = "spearman",
) -> tuple[DataFrame, int]:
    """
    Matriz de correlación (Spearman o Pearson) usando pyspark.ml.stat.Correlation.

    Descarta filas con cualquier nulo en las columnas indicadas.
    Devuelve (DataFrame con col_a, col_b, correlacion, metodo) y el número de filas usadas.

    Parámetros
    ----------
    df : DataFrame
    columnas : columnas numéricas
    metodo : "spearman" o "pearson"
    """
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.stat import Correlation

    df_limpio = df.dropna(subset=columnas)
    n_filas = df_limpio.count()

    assembler = VectorAssembler(inputCols=columnas, outputCol="_features")
    df_vec = assembler.transform(df_limpio).select("_features")

    matrix = Correlation.corr(df_vec, "_features", method=metodo).collect()[0][0]
    values = matrix.toArray()

    spark = df.sparkSession
    filas = []
    for i, col_a in enumerate(columnas):
        for j, col_b in enumerate(columnas):
            filas.append({
                "col_a": col_a,
                "col_b": col_b,
                "correlacion": float(values[i][j]),
                "metodo": metodo,
            })
    return spark.createDataFrame(filas, schema=["col_a", "col_b", "correlacion", "metodo"]), n_filas


def vif(
    df: DataFrame,
    columnas: List[str],
) -> DataFrame:
    """
    Factor de inflación de varianza (VIF) para columnas numéricas.

    Usa regresión OLS (GeneralizedLinearRegression binomial con logit) de cada variable
    contra las demás. VIF = 1 / (1 - R²).

    Referencia: Kutner et al., "Applied Linear Statistical Models", 5th ed.
    """
    from pyspark.ml.regression import LinearRegression
    from pyspark.ml.feature import VectorAssembler

    spark = df.sparkSession
    df_limpio = df.dropna(subset=columnas)
    filas = []

    for target in columnas:
        predictoras = [c for c in columnas if c != target]
        if not predictoras:
            continue
        assembler = VectorAssembler(inputCols=predictoras, outputCol="_feats")
        df_vec = assembler.transform(df_limpio)
        lr = LinearRegression(featuresCol="_feats", labelCol=target)
        modelo = lr.fit(df_vec)
        r2 = modelo.summary.r2
        vif_val = 1.0 / (1.0 - r2) if r2 < 1.0 else float("inf")
        filas.append({"columna": target, "vif": round(vif_val, 4)})

    return spark.createDataFrame(filas, schema=["columna", "vif"])
