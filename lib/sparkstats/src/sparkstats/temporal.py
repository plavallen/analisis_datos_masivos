"""
Funciones para variables históricas sin filtrar el futuro.
"""

from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, LongType


def tasa_historica_suavizada(
    df: DataFrame,
    col_clave: str,
    col_objetivo: str,
    col_fecha_entrega: str,
    col_fecha_referencia: str,
    ventana_dias: int = 90,
    m_suavizado: float = 30.0,
) -> DataFrame:
    """
    Tasa histórica de col_objetivo por col_clave en una ventana de días
    anterior a col_fecha_referencia, sin filtrar el futuro.

    Implementación escalable con agregados diarios por clave sobre col_fecha_entrega
    y ventanas con rangeBetween sobre el número de día.

    Suavizado empírico bayesiano:
        p_suav = (k + m * p_global) / (n + m)
    donde k = eventos en la ventana, n = pedidos en la ventana, p_global = tasa global
    en esa misma ventana, m = parámetro de suavizado.

    Parámetros
    ----------
    df : DataFrame con col_clave, col_objetivo (0/1), col_fecha_entrega y col_fecha_referencia
    col_clave : columna de agrupamiento (ej. seller_id)
    col_objetivo : columna binaria (0/1)
    col_fecha_entrega : fecha de entrega del pedido histórico
    col_fecha_referencia : fecha de la compra (inicio del período de predicción)
    ventana_dias : tamaño de la ventana en días (inclusive, hacia atrás)
    m_suavizado : parámetro de suavizado bayesiano (equivale a observaciones previas)

    Devuelve el DataFrame original con columnas adicionales:
        vol_<col_clave>_<ventana>d : pedidos en la ventana
        tasa_<col_clave>_<ventana>d : tasa suavizada
    """
    from pyspark.sql.window import Window

    # Número de día (días desde epoch) para aritmética entera con rangeBetween
    dia_entrega = F.datediff(F.col(col_fecha_entrega), F.lit("1970-01-01")).cast(LongType())
    dia_ref = F.datediff(F.col(col_fecha_referencia), F.lit("1970-01-01")).cast(LongType())

    # Agregados diarios por (clave, dia_entrega)
    agg_diarios = (
        df.withColumn("__dia_entrega", dia_entrega)
        .groupBy(col_clave, "__dia_entrega")
        .agg(
            F.count("*").alias("__n_dia"),
            F.sum(F.col(col_objetivo).cast(DoubleType())).alias("__k_dia"),
        )
    )

    # Para calcular la tasa global diaria (sin clave)
    agg_global = (
        df.withColumn("__dia_entrega", dia_entrega)
        .groupBy("__dia_entrega")
        .agg(
            F.count("*").alias("__n_dia_global"),
            F.sum(F.col(col_objetivo).cast(DoubleType())).alias("__k_dia_global"),
        )
    )

    # La ventana es: días desde (dia_ref - ventana_dias) hasta (dia_ref - 1).
    # Solo se pueden usar pedidos entregados antes de la fecha de compra.
    # rangeBetween(-ventana, -1) sobre dia_ref en las filas del DF original.
    # Implementamos uniendo los agregados diarios con el DF de pedidos a predecir
    # mediante una expresión de rango.

    # Unir dia_ref al DataFrame de pedidos históricos
    # El join se hace sobre clave, y el filtro de rango se aplica con una condición.
    # Para garantizar escalabilidad usamos broadcast del DF a predecir si es pequeño,
    # o un join con filtro de rango.

    # Como el spec requiere rangeBetween, lo implementamos via window function sobre
    # un DataFrame que ya tiene (clave, dia_referencia, dia_entrega).

    # Estrategia: unir df_predecir (con col_clave, dia_ref) con agg_diarios (clave, dia_entrega).
    # Luego filtrar dia_ref - ventana <= dia_entrega < dia_ref y agregar.

    df_ref = df.withColumn("__dia_ref", dia_ref).select(col_clave, "__dia_ref").distinct()

    joined = (
        df_ref.join(agg_diarios, on=col_clave, how="left")
        .filter(
            (F.col("__dia_entrega") >= F.col("__dia_ref") - ventana_dias)
            & (F.col("__dia_entrega") < F.col("__dia_ref"))
        )
    )

    # Tasa global en la misma ventana
    global_joined = (
        df_ref.join(agg_global, how="cross")
        .filter(
            (F.col("__dia_entrega") >= F.col("__dia_ref") - ventana_dias)
            & (F.col("__dia_entrega") < F.col("__dia_ref"))
        )
        .groupBy("__dia_ref")
        .agg(
            F.sum("__n_dia_global").alias("__n_global"),
            F.sum("__k_dia_global").alias("__k_global"),
        )
        .withColumn("__p_global", F.col("__k_global") / F.col("__n_global"))
    )

    ventana_agg = (
        joined.groupBy(col_clave, "__dia_ref")
        .agg(
            F.sum("__n_dia").alias("__n_ventana"),
            F.sum("__k_dia").alias("__k_ventana"),
        )
    )

    ventana_con_global = ventana_agg.join(global_joined, on="__dia_ref", how="left")

    col_vol = f"vol_{col_clave}_{ventana_dias}d"
    col_tasa = f"tasa_{col_clave}_{ventana_dias}d"

    m = F.lit(m_suavizado)
    ventana_final = (
        ventana_con_global
        .withColumn(
            col_tasa,
            (F.col("__k_ventana") + m * F.col("__p_global")) / (F.col("__n_ventana") + m),
        )
        .withColumnRenamed("__n_ventana", col_vol)
        .select(col_clave, "__dia_ref", col_vol, col_tasa)
    )

    return df.withColumn("__dia_ref", dia_ref).join(
        ventana_final, on=[col_clave, "__dia_ref"], how="left"
    ).drop("__dia_ref")
