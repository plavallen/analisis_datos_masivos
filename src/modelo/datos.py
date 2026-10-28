"""
Lectura y filtrado de población desde la capa silver.
"""

from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

from modelo.config import SILVER_PEDIDOS, SILVER_ENVIOS


def leer_silver(spark: SparkSession) -> tuple[DataFrame, DataFrame, dict]:
    """
    Lee las tablas silver.pedidos y silver.envios fijando el snapshot_id vigente.

    Devuelve (pedidos, envios, snapshots) donde snapshots es un dict
    {'pedidos': id, 'envios': id}.
    """
    snapshots = {}
    pedidos = spark.read.table(SILVER_PEDIDOS)
    snapshots["pedidos"] = _snapshot_vigente(spark, SILVER_PEDIDOS)

    envios = spark.read.table(SILVER_ENVIOS)
    snapshots["envios"] = _snapshot_vigente(spark, SILVER_ENVIOS)

    return pedidos, envios, snapshots


def _snapshot_vigente(spark: SparkSession, tabla: str) -> int | None:
    """Devuelve el snapshot_id del snapshot más reciente de una tabla Iceberg."""
    try:
        snaps = spark.sql(f"SELECT snapshot_id, committed_at FROM {tabla}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
        return int(snaps[0]["snapshot_id"]) if snaps else None
    except Exception:
        return None


def filtrar_poblacion(df: DataFrame) -> DataFrame:
    """
    Aplica los filtros de población en orden y devuelve el DataFrame filtrado.

    Filtros:
    1. Pedidos entregados con fecha de entrega no nula
    2. en_periodo_util == True
    3. Sin ninguna bandera de integridad temporal (flag_*)
    4. 'tarde' no nulo
    """
    flags = [c for c in df.columns if c.startswith("flag_")]

    df1 = df.filter(
        (F.col("order_status") == "delivered") & F.col("order_delivered_customer_date").isNotNull()
    )
    df2 = df1.filter(F.col("en_periodo_util") == True)

    filtro_flags = F.lit(False)
    for flag in flags:
        filtro_flags = filtro_flags | F.col(flag)
    df3 = df2.filter(~filtro_flags)

    df4 = df3.filter(F.col("tarde").isNotNull())

    return df4


def tabla_filtros(df_original: DataFrame, df_filtrado: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Tabla de diagnóstico que muestra cuántas filas quedan después de cada filtro.
    Devuelve un DataFrame con columnas [etapa, n_filas, n_descartadas].
    """
    flags = [c for c in df_original.columns if c.startswith("flag_")]
    filtro_flags = F.lit(False)
    for flag in flags:
        filtro_flags = filtro_flags | F.col(flag)

    entregados = df_original.filter(
        (F.col("order_status") == "delivered") & F.col("order_delivered_customer_date").isNotNull()
    )
    en_periodo = entregados.filter(F.col("en_periodo_util") == True)
    sin_flags = en_periodo.filter(~filtro_flags)
    con_tarde = sin_flags.filter(F.col("tarde").isNotNull())

    etapas = [
        ("total_original", df_original.count()),
        ("entregados_con_fecha", entregados.count()),
        ("en_periodo_util", en_periodo.count()),
        ("sin_flags_integridad", sin_flags.count()),
        ("tarde_no_nulo", con_tarde.count()),
    ]

    filas = []
    prev = etapas[0][1]
    for nombre, n in etapas:
        filas.append({"etapa": nombre, "n_filas": n, "n_descartadas": prev - n})
        prev = n

    return spark.createDataFrame(filas, schema=["etapa", "n_filas", "n_descartadas"])
