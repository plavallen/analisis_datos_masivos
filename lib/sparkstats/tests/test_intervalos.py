"""Tests para sparkstats.intervalos."""

import pytest
from sparkstats.intervalos import wilson, tasa_por_grupo


def test_wilson_valor_referencia(spark):
    """
    Intervalo de Wilson para p=0.5, n=100 debe contener 0.5 y ser cercano
    al valor tabulado [0.4002, 0.5998] al 95%.
    """
    df = spark.createDataFrame([{"exitos": 50, "total": 100}])
    resultado = wilson(df, "exitos", "total", nivel_confianza=0.95).collect()[0]
    assert resultado["wilson_lo"] < 0.5 < resultado["wilson_hi"]
    assert abs(resultado["wilson_lo"] - 0.4002) < 0.01
    assert abs(resultado["wilson_hi"] - 0.5998) < 0.01


def test_wilson_extremos_validos(spark):
    """wilson_lo >= 0 y wilson_hi <= 1 siempre."""
    df = spark.createDataFrame([{"e": 0, "n": 50}, {"e": 50, "n": 50}])
    resultado = wilson(df, "e", "n").collect()
    for r in resultado:
        assert r["wilson_lo"] >= 0.0
        assert r["wilson_hi"] <= 1.0


def test_tasa_por_grupo_nulos(spark):
    """Los nulos en la columna de grupo se cuentan como "NA"."""
    from pyspark.sql.types import StringType
    df = spark.createDataFrame(
        [(None, 1), ("A", 0), ("A", 1), (None, 0)],
        schema=["grupo", "objetivo"],
    )
    resultado = tasa_por_grupo(df, "grupo", "objetivo")
    grupos = [r["grupo"] for r in resultado.collect()]
    assert "NA" in grupos
