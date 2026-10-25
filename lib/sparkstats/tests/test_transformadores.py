"""Tests para sparkstats.transformadores."""

import pytest
from sparkstats.transformadores import AgruparRaras, RecortarPercentiles


def test_agrupar_raras_aprende_solo_de_fit(spark):
    """
    AgruparRaras aprende solo con los datos de fit.
    Las categorías del conjunto de transform que no estuvieron en fit
    y tienen pocos eventos deben quedar como "otras".
    """
    train = spark.createDataFrame(
        [("A", 1)] * 25 + [("B", 1)] * 25 + [("C", 0)] * 5,
        schema=["categoria", "tarde"],
    )
    test = spark.createDataFrame(
        [("A", None), ("C", None), ("D", None)],
        schema=["categoria", "tarde"],
    )

    modelo = AgruparRaras(
        inputCol="categoria", outputCol="cat_agrupada", colObjetivo="tarde", minEventos=20
    ).fit(train)

    resultado = modelo.transform(test).select("cat_agrupada").collect()
    mapa = {r["cat_agrupada"] for r in resultado}

    assert "A" in mapa, "A tiene suficientes eventos y debe mantenerse"
    assert "otras" in mapa, "C y D tienen pocos/ningún evento y deben quedar como 'otras'"


def test_recortar_percentiles_aprende_solo_de_fit(spark):
    """
    RecortarPercentiles aprende p01 y p99 solo del conjunto de entrenamiento.
    Los valores de test fuera del rango aprendido deben ser recortados.
    """
    train = spark.createDataFrame([(float(i),) for i in range(100)], schema=["valor"])
    test = spark.createDataFrame([(-1000.0,), (50.0,), (1000.0,)], schema=["valor"])

    modelo = RecortarPercentiles(inputCol="valor", outputCol="valor_recortado").fit(train)

    resultado = modelo.transform(test).select("valor_recortado").collect()
    valores = [r["valor_recortado"] for r in resultado]

    # Los extremos de train son 0 a 99; p01 ~= 0.99, p99 ~= 98.01
    for v in valores:
        assert 0.0 <= v <= 99.0, f"Valor {v} fuera del rango aprendido de entrenamiento"


def test_pipeline_guardar_cargar(spark, tmp_path):
    """
    Un Pipeline con AgruparRaras y RecortarPercentiles guardado y cargado
    produce predicciones idénticas.
    """
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import StringIndexer, VectorAssembler
    from pyspark.ml.classification import LogisticRegression

    train = spark.createDataFrame(
        [("A", 1.0, 1), ("A", 2.0, 0), ("B", 3.0, 1), ("B", 4.0, 0)] * 10,
        schema=["cat", "num", "tarde"],
    )

    pipeline = Pipeline(stages=[
        AgruparRaras(inputCol="cat", outputCol="cat_ag", colObjetivo="tarde", minEventos=5),
        RecortarPercentiles(inputCol="num", outputCol="num_r"),
        StringIndexer(inputCol="cat_ag", outputCol="cat_idx", handleInvalid="keep"),
        VectorAssembler(inputCols=["cat_idx", "num_r"], outputCol="features"),
        LogisticRegression(featuresCol="features", labelCol="tarde"),
    ])

    modelo = pipeline.fit(train)
    pred_original = modelo.transform(train).select("prediction").collect()

    ruta = str(tmp_path / "pipeline_test")
    modelo.save(ruta)

    from pyspark.ml import PipelineModel
    modelo_cargado = PipelineModel.load(ruta)
    pred_cargado = modelo_cargado.transform(train).select("prediction").collect()

    for orig, carg in zip(pred_original, pred_cargado):
        assert orig["prediction"] == carg["prediction"], "Las predicciones deben ser idénticas"
