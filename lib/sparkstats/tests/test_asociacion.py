"""Tests para sparkstats.asociacion."""

import pytest
from sparkstats.asociacion import cramer_v, matriz_cramer


def test_cramer_v_identicas(spark):
    """V de Cramér vale 1 para dos columnas idénticas."""
    df = spark.createDataFrame(
        [(str(i % 3),) for i in range(300)],
        schema=["categoria"],
    ).withColumn("copia", __import__("pyspark.sql.functions", fromlist=["col"]).col("categoria"))
    v = cramer_v(df, "categoria", "copia", correccion_bergsma=False)
    assert abs(v - 1.0) < 0.05, f"Se esperaba ~1.0, se obtuvo {v}"


def test_cramer_v_independientes(spark):
    """V de Cramér vale cerca de 0 para columnas independientes."""
    import random
    random.seed(42)
    datos = [(random.choice(["A", "B", "C"]), random.choice(["X", "Y", "Z"])) for _ in range(1000)]
    df = spark.createDataFrame(datos, schema=["col_a", "col_b"])
    v = cramer_v(df, "col_a", "col_b", correccion_bergsma=True)
    assert v < 0.15, f"Se esperaba cerca de 0, se obtuvo {v}"


def test_matriz_cramer_dimensiones(spark):
    """La matriz triangular inferior tiene n*(n-1)/2 filas."""
    df = spark.createDataFrame(
        [(str(i % 3), str(i % 4), str(i % 2)) for i in range(300)],
        schema=["a", "b", "c"],
    )
    matriz = matriz_cramer(df, ["a", "b", "c"])
    assert matriz.count() == 3
