"""
Construcción de variables para el modelo: envío crítico, variables derivadas
e historial del vendedor sin filtrar el futuro.
"""

from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType

from sparkstats.temporal import tasa_historica_suavizada
from modelo.config import (
    VENTANA_HISTORIAL_DIAS,
    M_SUAVIZADO,
    MIN_PEDIDOS_RUTA,
    MIN_PEDIDOS_ESTADO,
)


def envio_critico(envios: DataFrame) -> DataFrame:
    """
    Para cada order_id, elige el envío con mayor distancia_km (envío crítico).
    Usa max sobre un struct para que todos los atributos salgan del mismo envío.

    Devuelve un DataFrame con columnas:
        order_id, distancia_critica_km, seller_id_critico, seller_state_critico,
        n_estados_vendedor, distancia_incompleta.
    """
    return (
        envios
        .withColumn(
            "__max_struct",
            F.max(F.struct("distancia_km", "seller_id", "seller_state")).over(
                __window("order_id")
            ),
        )
        .groupBy("order_id")
        .agg(
            F.max(F.struct("distancia_km", "seller_id", "seller_state")).alias("__crit"),
            F.countDistinct("seller_state").alias("n_estados_vendedor"),
            F.sum(F.when(F.col("distancia_km").isNull(), 1).otherwise(0)).alias("__sin_dist"),
        )
        .withColumn("distancia_critica_km", F.col("__crit.distancia_km"))
        .withColumn("seller_id_critico", F.col("__crit.seller_id"))
        .withColumn("seller_state_critico", F.col("__crit.seller_state"))
        .withColumn("distancia_incompleta", F.col("__sin_dist") > 0)
        .drop("__crit", "__sin_dist")
    )


def __window(col_part: str):
    from pyspark.sql.window import Window
    return Window.partitionBy(col_part)


def variables_pedido(pedidos: DataFrame, envios_criticos: DataFrame) -> DataFrame:
    """
    Une pedidos con los atributos del envío crítico y deriva variables de fecha.

    Variables derivadas:
    - mes_del_anio (1-12)
    - dia_semana (1=lunes, 7=domingo)
    - mismo_estado: customer_state == seller_state_critico
    """
    return (
        pedidos
        .join(envios_criticos, on="order_id", how="left")
        .withColumn("mes_del_anio", F.month("fecha_compra"))
        .withColumn("dia_semana", F.dayofweek("fecha_compra"))
        .withColumn(
            "mismo_estado",
            F.col("customer_state") == F.col("seller_state_critico"),
        )
    )


def historial_vendedor(df: DataFrame) -> DataFrame:
    """
    Agrega el historial del vendedor en una ventana de VENTANA_HISTORIAL_DIAS días
    anteriores a la fecha de compra, sin filtrar el futuro.

    Requiere que el DataFrame tenga las columnas:
    - seller_id_critico
    - tarde (0/1)
    - order_delivered_customer_date (fecha de entrega real, puede ser nula en el set a predecir)
    - fecha_compra

    Las filas sin fecha de entrega (a predecir) participan de la predicción pero no
    del historial, ya que se filtran por la condición de que la entrega sea anterior.
    """
    return tasa_historica_suavizada(
        df=df,
        col_clave="seller_id_critico",
        col_objetivo="tarde",
        col_fecha_entrega="order_delivered_customer_date",
        col_fecha_referencia="fecha_compra",
        ventana_dias=VENTANA_HISTORIAL_DIAS,
        m_suavizado=M_SUAVIZADO,
    )


def margen_promesa(df: DataFrame) -> DataFrame:
    """
    Calcula el margen de la promesa:
        margen_promesa = dias_prometidos - mediana_historica_t_total_en_ruta

    Usa pedidos entregados antes de la fecha de compra en una ventana de 90 días.
    Si la ruta tiene menos de MIN_PEDIDOS_RUTA, usa el estado del cliente.
    Si el estado tampoco alcanza, usa la global.

    Agrega columna 'nivel_margen' con el nivel usado: 'ruta', 'estado' o 'global'.
    """
    from pyspark.sql.window import Window

    dia_base = F.lit("1970-01-01")
    dia_entrega = F.datediff("order_delivered_customer_date", dia_base)
    dia_compra = F.datediff("fecha_compra", dia_base)

    df_hist = df.withColumn("__dia_entrega", dia_entrega).withColumn("__dia_compra", dia_compra)

    ventana = VENTANA_HISTORIAL_DIAS
    min_ruta = MIN_PEDIDOS_RUTA
    min_estado = MIN_PEDIDOS_ESTADO

    # Agregado por ruta (seller_state + customer_state), período de ventana
    # Usamos un join cruzado: para cada pedido a predecir, buscamos pedidos históricos
    # dentro de la ventana en la misma ruta.
    df_ref = df_hist.select(
        "order_id", "__dia_compra",
        "seller_state_critico", "customer_state", "dias_prometidos",
    ).distinct()

    df_historial = df_hist.filter(
        F.col("order_delivered_customer_date").isNotNull()
    ).select(
        "seller_state_critico", "customer_state",
        "__dia_entrega", "t_total",
    )

    # Mediana por ruta
    ruta_mediana = (
        df_ref.alias("ref")
        .join(
            df_historial.alias("hist"),
            on=[
                F.col("ref.seller_state_critico") == F.col("hist.seller_state_critico"),
                F.col("ref.customer_state") == F.col("hist.customer_state"),
                F.col("hist.__dia_entrega") >= F.col("ref.__dia_compra") - ventana,
                F.col("hist.__dia_entrega") < F.col("ref.__dia_compra"),
            ],
            how="left",
        )
        .groupBy("ref.order_id")
        .agg(
            F.percentile_approx("hist.t_total", 0.5).alias("__med_ruta"),
            F.count("hist.t_total").alias("__n_ruta"),
        )
    )

    # Mediana por estado destino
    estado_mediana = (
        df_ref.alias("ref")
        .join(
            df_historial.alias("hist"),
            on=[
                F.col("ref.customer_state") == F.col("hist.customer_state"),
                F.col("hist.__dia_entrega") >= F.col("ref.__dia_compra") - ventana,
                F.col("hist.__dia_entrega") < F.col("ref.__dia_compra"),
            ],
            how="left",
        )
        .groupBy("ref.order_id")
        .agg(
            F.percentile_approx("hist.t_total", 0.5).alias("__med_estado"),
            F.count("hist.t_total").alias("__n_estado"),
        )
    )

    # Mediana global
    global_mediana = (
        df_ref.alias("ref")
        .join(
            df_historial.alias("hist"),
            on=[
                F.col("hist.__dia_entrega") >= F.col("ref.__dia_compra") - ventana,
                F.col("hist.__dia_entrega") < F.col("ref.__dia_compra"),
            ],
            how="left",
        )
        .groupBy("ref.order_id")
        .agg(
            F.percentile_approx("hist.t_total", 0.5).alias("__med_global"),
        )
    )

    return (
        df.join(ruta_mediana, on="order_id", how="left")
        .join(estado_mediana, on="order_id", how="left")
        .join(global_mediana, on="order_id", how="left")
        .withColumn(
            "nivel_margen",
            F.when(F.col("__n_ruta") >= min_ruta, "ruta")
             .when(F.col("__n_estado") >= min_estado, "estado")
             .otherwise("global"),
        )
        .withColumn(
            "__mediana_referencia",
            F.when(F.col("__n_ruta") >= min_ruta, F.col("__med_ruta"))
             .when(F.col("__n_estado") >= min_estado, F.col("__med_estado"))
             .otherwise(F.col("__med_global")),
        )
        .withColumn(
            "margen_promesa",
            F.col("dias_prometidos").cast(DoubleType()) - F.col("__mediana_referencia"),
        )
        .drop("__med_ruta", "__med_estado", "__med_global", "__n_ruta", "__n_estado")
    )
