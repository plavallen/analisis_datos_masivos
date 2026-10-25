"""
Transformadores reutilizables compatibles con el Pipeline de PySpark ML.
Persistibles con DefaultParamsReadable y DefaultParamsWritable.
"""

from __future__ import annotations

from pyspark.ml import Estimator, Model
from pyspark.ml.param.shared import (
    HasInputCol,
    HasOutputCol,
    Params,
    Param,
    TypeConverters,
)
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, StringType


class AgruparRarasModel(
    Model,
    HasInputCol,
    HasOutputCol,
    DefaultParamsReadable,
    DefaultParamsWritable,
):
    """Modelo aprendido por AgruparRaras."""

    categorias_frecuentes = Param(
        Params._dummy(),
        "categorias_frecuentes",
        "Lista de categorías con suficientes eventos.",
        typeConverter=TypeConverters.toListString,
    )

    def __init__(self, inputCol="", outputCol="", categorias_frecuentes=None):
        super().__init__()
        self._set(
            inputCol=inputCol,
            outputCol=outputCol,
            categorias_frecuentes=categorias_frecuentes or [],
        )

    def _transform(self, df: DataFrame) -> DataFrame:
        cat_set = set(self.getOrDefault("categorias_frecuentes"))
        col_in = self.getInputCol()
        col_out = self.getOutputCol()
        return df.withColumn(
            col_out,
            F.when(
                F.col(col_in).cast(StringType()).isin(list(cat_set)),
                F.col(col_in).cast(StringType()),
            ).otherwise(F.lit("otras")),
        )


class AgruparRaras(
    Estimator,
    HasInputCol,
    HasOutputCol,
    DefaultParamsReadable,
    DefaultParamsWritable,
):
    """
    Aprende qué categorías tienen al menos min_eventos en col_objetivo
    y en transform reemplaza las demás por "otras".

    Parámetros
    ----------
    inputCol : columna categórica de entrada
    outputCol : columna de salida con raras agrupadas
    colObjetivo : nombre de la columna de objetivo binario (0/1)
    minEventos : mínimo de eventos para no ser "rara"
    """

    colObjetivo = Param(
        Params._dummy(), "colObjetivo", "Columna de objetivo binario.", typeConverter=TypeConverters.toString
    )
    minEventos = Param(
        Params._dummy(), "minEventos", "Mínimo de eventos para no agrupar.", typeConverter=TypeConverters.toInt
    )

    def __init__(self, inputCol="", outputCol="", colObjetivo="", minEventos=20):
        super().__init__()
        self._set(inputCol=inputCol, outputCol=outputCol, colObjetivo=colObjetivo, minEventos=minEventos)

    def _fit(self, df: DataFrame) -> AgruparRarasModel:
        col_in = self.getInputCol()
        col_obj = self.getOrDefault("colObjetivo")
        min_ev = self.getOrDefault("minEventos")

        frecuentes = (
            df.withColumn(col_in, F.coalesce(F.col(col_in).cast(StringType()), F.lit("NA")))
            .groupBy(col_in)
            .agg(F.sum(F.col(col_obj).cast(DoubleType())).alias("eventos"))
            .filter(F.col("eventos") >= min_ev)
            .select(col_in)
            .rdd.flatMap(lambda r: [r[0]])
            .collect()
        )
        return AgruparRarasModel(
            inputCol=self.getInputCol(),
            outputCol=self.getOutputCol(),
            categorias_frecuentes=frecuentes,
        )


class RecortarPercentilesModel(
    Model,
    HasInputCol,
    HasOutputCol,
    DefaultParamsReadable,
    DefaultParamsWritable,
):
    """Modelo aprendido por RecortarPercentiles."""

    p01 = Param(Params._dummy(), "p01", "Percentil 1 aprendido.", typeConverter=TypeConverters.toFloat)
    p99 = Param(Params._dummy(), "p99", "Percentil 99 aprendido.", typeConverter=TypeConverters.toFloat)

    def __init__(self, inputCol="", outputCol="", p01=0.0, p99=1.0):
        super().__init__()
        self._set(inputCol=inputCol, outputCol=outputCol, p01=p01, p99=p99)

    def _transform(self, df: DataFrame) -> DataFrame:
        lo = self.getOrDefault("p01")
        hi = self.getOrDefault("p99")
        col_in = self.getInputCol()
        col_out = self.getOutputCol()
        return df.withColumn(
            col_out,
            F.greatest(F.lit(float(lo)), F.least(F.lit(float(hi)), F.col(col_in).cast(DoubleType()))),
        )


class RecortarPercentiles(
    Estimator,
    HasInputCol,
    HasOutputCol,
    DefaultParamsReadable,
    DefaultParamsWritable,
):
    """
    Aprende p01 y p99 en fit y recorta en transform.
    Útil para variables con colas largas como diff_pago.
    """

    def __init__(self, inputCol="", outputCol=""):
        super().__init__()
        self._set(inputCol=inputCol, outputCol=outputCol)

    def _fit(self, df: DataFrame) -> RecortarPercentilesModel:
        col_in = self.getInputCol()
        p01, p99 = df.approxQuantile(col_in, [0.01, 0.99], 0.001)
        return RecortarPercentilesModel(
            inputCol=self.getInputCol(),
            outputCol=self.getOutputCol(),
            p01=float(p01),
            p99=float(p99),
        )
